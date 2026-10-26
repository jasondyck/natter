//! The muxers' files, checked with ffprobe/ffmpeg where installed.

use std::io::Cursor;
use std::path::Path;
use std::process::Command;

use super::ogg::{self, OggOpus};
use super::webm::{Track, Webm};

fn have(tool: &str) -> bool {
    Command::new(tool).arg("-version").output().is_ok()
}

/// ffmpeg decodes the whole file without complaint.
fn decodes(path: &Path) {
    let out = Command::new("ffmpeg")
        .args(["-v", "error", "-i"])
        .arg(path)
        .args(["-f", "null", "-"])
        .output()
        .expect("ffmpeg");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success() && err.trim().is_empty(), "{err}");
}

/// ffprobe's format duration, in seconds.
fn duration(path: &Path) -> f64 {
    let out = Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "csv=p=0",
        ])
        .arg(path)
        .output()
        .expect("ffprobe");
    String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse()
        .expect("duration")
}

fn tmp(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("natter-egress-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name)
}

#[test]
fn opus_samples_from_toc() {
    assert_eq!(ogg::packet_samples(&[0xf8, 0xff, 0xfe]), Some(960));
    // SILK 20 ms, two frames.
    assert_eq!(ogg::packet_samples(&[0x09]), Some(1920));
    // Hybrid 10 ms.
    assert_eq!(ogg::packet_samples(&[0x60]), Some(480));
    // Code 3, 3 frames of CELT 2.5 ms.
    assert_eq!(ogg::packet_samples(&[0x83, 0x03]), Some(360));
    assert_eq!(ogg::packet_samples(&[]), None);
}

/// 10 s of 20 ms frames with a 2 s hole (DTX): silence fills it, and late
/// and repeated packets are dropped.
#[test]
fn ogg_fills_gaps() {
    let frame = [0xf8, 0xff, 0xfe];
    let mut w = OggOpus::new(Cursor::new(vec![]), 7);
    let start = u32::MAX - 960 * 10; // wraps
    for n in 0..500u32 {
        if (100..200).contains(&n) {
            continue;
        }
        let ts = start.wrapping_add(n * 960);
        w.push(ts, &frame).unwrap();
        if n == 300 {
            w.push(ts, &frame).unwrap();
            w.push(ts.wrapping_sub(960 * 3), &frame).unwrap();
        }
    }
    assert_eq!(w.duration(), std::time::Duration::from_secs(10));
    let bytes = w.finish().unwrap().into_inner();
    let pages = parse_pages(&bytes);
    assert_eq!(pages.first().map(|p| p.0), Some(0x02));
    assert_eq!(pages.last().map(|p| p.0), Some(0x04));
    // The last granule: 10 s plus pre-skip.
    assert_eq!(pages.last().map(|p| p.1), Some(480_000 + 312));
    if have("ffprobe") {
        let path = tmp("gaps.ogg");
        std::fs::write(&path, &bytes).unwrap();
        decodes(&path);
        let d = duration(&path);
        assert!((d - 10.0).abs() < 0.05, "{d}");
    }
}

/// (flags, granule) of each page, checking capture pattern and CRC.
fn parse_pages(mut b: &[u8]) -> Vec<(u8, u64)> {
    let mut pages = vec![];
    while !b.is_empty() {
        assert_eq!(&b[..4], b"OggS");
        let n = usize::from(b[26]);
        let body: usize = b[27..27 + n].iter().map(|&x| usize::from(x)).sum();
        let len = 27 + n + body;
        let mut page = b[..len].to_vec();
        let crc = u32::from_le_bytes(page[22..26].try_into().unwrap());
        page[22..26].fill(0);
        assert_eq!(crc, ogg_crc(&page));
        pages.push((b[5], u64::from_le_bytes(b[6..14].try_into().unwrap())));
        b = &b[len..];
    }
    pages
}

fn ogg_crc(data: &[u8]) -> u32 {
    let mut crc = 0u32;
    for &byte in data {
        crc ^= u32::from(byte) << 24;
        for _ in 0..8 {
            crc = if crc & 0x8000_0000 != 0 {
                (crc << 1) ^ 0x04c1_1db7
            } else {
                crc << 1
            };
        }
    }
    crc
}

/// VP8 and VP9 frames from ffmpeg (IVF), muxed to WebM with silent Opus
/// alongside: decodes, lasts as long, and seeks.
#[test]
fn webm_vp8() {
    webm_video(
        "vp8",
        "libvpx",
        |f| f[0] & 1 == 0,
        |w, h| Track::Vp8 {
            width: w,
            height: h,
        },
    );
}

#[test]
fn webm_vp9() {
    // VP9 uncompressed header: frame marker, profile 0, show_existing_frame
    // 0, then frame_type 0 for a keyframe.
    webm_video(
        "vp9",
        "libvpx-vp9",
        |f| f[0] & 0x04 == 0,
        |w, h| Track::Vp9 {
            width: w,
            height: h,
        },
    );
}

fn webm_video(name: &str, encoder: &str, is_key: fn(&[u8]) -> bool, track: fn(u32, u32) -> Track) {
    if !have("ffmpeg") {
        return;
    }
    let ivf = tmp(&format!("{name}.ivf"));
    let ok = Command::new("ffmpeg")
        .args([
            "-v",
            "error",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=320x240:rate=30",
            "-t",
            "7",
        ])
        .args(["-c:v", encoder, "-g", "60", "-b:v", "300k"])
        .arg(&ivf)
        .status()
        .unwrap();
    assert!(ok.success());
    let data = std::fs::read(&ivf).unwrap();
    let mut at = usize::from(u16::from_le_bytes([data[6], data[7]]));
    let path = tmp(&format!("{name}.webm"));
    let file = std::fs::File::create(&path).unwrap();
    let tracks = vec![track(320, 240), Track::Opus { channels: 1 }];
    let mut w = Webm::new(file, tracks).unwrap();
    let mut frames = 0u64;
    let mut audio_ms = 0;
    while at + 12 <= data.len() {
        let len = u32::from_le_bytes(data[at..at + 4].try_into().unwrap()) as usize;
        let frame = &data[at + 12..at + 12 + len];
        let ms = frames * 1000 / 30;
        while audio_ms <= ms {
            w.frame(2, audio_ms, false, &[0xf8, 0xff, 0xfe]).unwrap();
            audio_ms += 20;
        }
        w.frame(1, ms, is_key(frame), frame).unwrap();
        frames += 1;
        at += 12 + len;
    }
    assert_eq!(frames, 210);
    w.finish(7000).unwrap();
    decodes(&path);
    let d = duration(&path);
    assert!((d - 7.0).abs() < 0.05, "{d}");
    // Seeking to 5 s lands on the keyframe at 4 s (the Cues).
    let out = Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-select_streams",
            "v",
            "-read_intervals",
            "5%+#1",
        ])
        .args(["-show_entries", "packet=pts_time,flags"])
        .args(["-of", "csv=p=0"])
        .arg(&path)
        .output()
        .unwrap();
    let first = String::from_utf8_lossy(&out.stdout);
    assert!(
        first.lines().any(|l| l.starts_with("4.000000,K")),
        "{first}"
    );
}

#[test]
fn file_paths() {
    let r = super::Recordings::new("/rec".into(), None);
    let reps = [("{room_name}", "lobby"), ("{track_id}", "TR_A")];
    let path = |p: &str| r.file_path(p, "TR_A", &reps, ".ogg");
    let (full, name) = path("{room_name}/{track_id}").unwrap();
    assert_eq!(
        (full.to_str().unwrap(), name.as_str()),
        ("/rec/lobby/TR_A.ogg", "lobby/TR_A.ogg")
    );
    // LiveKit's: a known extension is replaced, another kept.
    assert_eq!(path("a.mp4").unwrap().1, "a.ogg");
    assert_eq!(path("a.x").unwrap().1, "a.x.ogg");
    // No name, or a directory: identifier and time.
    for (req, prefix) in [("", "TR_A-"), ("d/", "d/TR_A-")] {
        let name = path(req).unwrap().1;
        let rest = name.strip_prefix(prefix).unwrap();
        assert_eq!(
            (rest.len(), &rest[10..11], &rest[17..]),
            (21, "T", ".ogg"),
            "{name}"
        );
    }
    for bad in ["../x", "/etc/x", "a/../../x"] {
        assert!(path(bad).is_err(), "{bad}");
    }
}

#[test]
fn dates() {
    assert_eq!(super::civil_from_days(0), (1970, 1, 1));
    assert_eq!(super::civil_from_days(19_723), (2024, 1, 1));
    assert_eq!(super::civil_from_days(19_782), (2024, 2, 29));
}
