from flask import Flask, render_template, request, send_file, jsonify, Response
import subprocess
import os
import uuid
import shutil
import re
import tempfile
import json
import threading
import time
from pathlib import Path

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 320 * 1024 * 1024 * 1024

jobs = {}
jobs_lock = threading.Lock()

import multiprocessing
CPU_CORES  = multiprocessing.cpu_count()
FF_THREADS = max(1, CPU_CORES - 1)
MAX_EPISODES = 40


# ── FFmpeg checks ─────────────────────────────────────────────
def check_ffmpeg():
    try:
        subprocess.run(['ffmpeg', '-version'],
                       capture_output=True, check=True, timeout=10)
        return True
    except Exception:
        return False


def check_hw_accel():
    hw = {'nvenc': False, 'qsv': False,
          'videotoolbox': False, 'vaapi': False}
    try:
        r = subprocess.run(['ffmpeg', '-encoders'],
                           capture_output=True, text=True, timeout=10)
        out = r.stdout
        if 'h264_nvenc'        in out: hw['nvenc']        = True
        if 'h264_qsv'          in out: hw['qsv']          = True
        if 'h264_videotoolbox' in out: hw['videotoolbox'] = True
        if 'h264_vaapi'        in out: hw['vaapi']        = True
    except Exception:
        pass
    return hw


HW_ACCEL = check_hw_accel()
print(f"[HW]  Detected : {HW_ACCEL}")
print(f"[CPU] Threads  : {FF_THREADS}  (cores={CPU_CORES})")


def get_best_encoder():
    if HW_ACCEL['nvenc']:
        return 'h264_nvenc', ['-preset', 'p1', '-tune', 'll',
                              '-b:v', '0', '-cq', '28']
    if HW_ACCEL['videotoolbox']:
        return 'h264_videotoolbox', ['-q:v', '55', '-realtime', '1']
    if HW_ACCEL['qsv']:
        return 'h264_qsv', ['-preset', 'veryfast',
                            '-global_quality', '28']
    if HW_ACCEL['vaapi']:
        return 'h264_vaapi', ['-qp', '28']
    return 'libx264', ['-preset', 'ultrafast', '-crf', '26',
                       '-threads', str(FF_THREADS),
                       '-tune', 'fastdecode']


# ── Path helpers ──────────────────────────────────────────────
def validate_save_path(path_str):
    if not path_str or not path_str.strip():
        return None, "Save path cannot be empty"
    path_str = path_str.strip()
    path_str = os.path.expanduser(path_str)
    path_str = os.path.expandvars(path_str)
    try:
        p = Path(path_str).resolve()
    except Exception as ex:
        return None, f"Invalid path: {ex}"
    if p.is_file():
        return None, f"Path is a file, not a folder: {p}"
    if not p.exists():
        try:
            p.mkdir(parents=True, exist_ok=True)
        except PermissionError:
            return None, f"Permission denied: {p}"
        except Exception as ex:
            return None, f"Cannot create folder: {ex}"
    test_file = p / f'.write_test_{uuid.uuid4().hex[:6]}'
    try:
        test_file.touch()
        test_file.unlink()
    except Exception:
        return None, f"Folder not writable: {p}"
    return str(p), None


# ── SRT / ASS helpers ─────────────────────────────────────────
def parse_srt(path):
    encodings = ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']
    content = None
    for enc in encodings:
        try:
            with open(path, 'r', encoding=enc) as f:
                content = f.read()
            break
        except Exception:
            continue
    if not content:
        with open(path, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read()
    content = (content.replace('\ufeff', '')
                      .replace('\r\n', '\n')
                      .replace('\r', '\n'))
    entries = []
    for block in re.split(r'\n\s*\n', content.strip()):
        lines = block.strip().split('\n')
        if len(lines) < 2:
            continue
        time_match = None
        time_idx   = -1
        for li, line in enumerate(lines):
            m = re.match(
                r'(\d{2}):(\d{2}):(\d{2})[,.](\d{3})\s*-->\s*'
                r'(\d{2}):(\d{2}):(\d{2})[,.](\d{3})', line.strip())
            if m:
                time_match = m
                time_idx   = li
                break
        if not time_match:
            continue
        g     = time_match.groups()
        start = (int(g[0])*3600 + int(g[1])*60
                 + int(g[2]) + int(g[3])/1000)
        end   = (int(g[4])*3600 + int(g[5])*60
                 + int(g[6]) + int(g[7])/1000)
        text  = '\n'.join(lines[time_idx+1:])
        text  = re.sub(r'<[^>]+>', '', text)
        text  = re.sub(r'\{[^}]+\}', '', text).strip()
        if text:
            entries.append({'start': start, 'end': end, 'text': text})
    return entries


def format_srt_time(s):
    h  = int(s // 3600)
    m  = int((s % 3600) // 60)
    sc = int(s % 60)
    ms = int((s % 1) * 1000)
    return f"{h:02d}:{m:02d}:{sc:02d},{ms:03d}"


def format_ass_time(s):
    h  = int(s // 3600)
    m  = int((s % 3600) // 60)
    sc = int(s % 60)
    cs = int((s % 1) * 100)
    return f"{h}:{m:02d}:{sc:02d}.{cs:02d}"


def clean_srt(src, dst):
    entries = parse_srt(src)
    with open(dst, 'w', encoding='utf-8') as f:
        for i, e in enumerate(entries, 1):
            f.write(f"{i}\n{format_srt_time(e['start'])} --> "
                    f"{format_srt_time(e['end'])}\n{e['text']}\n\n")
    return len(entries)


def create_ass(srt_path, ass_path, w=1920, h=1080):
    entries   = parse_srt(srt_path)
    font_size = max(int(h * 0.045), 24)
    margin_v  = int(h * 0.06)
    margin_lr = int(w * 0.05)
    header = (
        "[Script Info]\n"
        "ScriptType: v4.00+\n"
        f"PlayResX: {w}\n"
        f"PlayResY: {h}\n"
        "WrapStyle: 0\n"
        "ScaledBorderAndShadow: yes\n\n"
        "[V4+ Styles]\n"
        "Format: Name, Fontname, Fontsize, PrimaryColour, "
        "SecondaryColour, OutlineColour, BackColour, Bold, Italic, "
        "Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, "
        "BorderStyle, Outline, Shadow, Alignment, "
        "MarginL, MarginR, MarginV, Encoding\n"
        f"Style: Default,Arial,{font_size},"
        f"&H00FFFFFF,&H000000FF,&H00000000,&H96000000,"
        f"0,0,0,0,100,100,0,0,1,2,1,2,"
        f"{margin_lr},{margin_lr},{margin_v},1\n\n"
        "[Events]\n"
        "Format: Layer, Start, End, Style, Name, "
        "MarginL, MarginR, MarginV, Effect, Text\n"
    )
    dialogue_lines = []
    for e in entries:
        start = format_ass_time(e['start'])
        end   = format_ass_time(e['end'])
        text  = (e['text'].replace('\n', '\\N')
                          .replace('{', '').replace('}', ''))
        dialogue_lines.append(
            f"Dialogue: 0,{start},{end},Default,,0,0,0,,{text}\n")
    with open(ass_path, 'w', encoding='utf-8') as f:
        f.write(header)
        f.writelines(dialogue_lines)
    return len(entries)


def get_video_info(path):
    try:
        r = subprocess.run(
            ['ffprobe', '-v', 'quiet', '-print_format', 'json',
             '-show_streams', '-show_format', path],
            capture_output=True, text=True, timeout=30)
        if r.returncode == 0:
            data = json.loads(r.stdout)
            w, h, dur = 1920, 1080, 0
            for s in data.get('streams', []):
                if s.get('codec_type') == 'video':
                    w   = int(s.get('width',  1920))
                    h   = int(s.get('height', 1080))
                    dur = float(s.get('duration', 0))
            if dur == 0:
                dur = float(data.get('format', {}).get('duration', 0))
            return {'width': w, 'height': h, 'duration': dur}
    except Exception as ex:
        print(f"[get_video_info] {ex}")
    return {'width': 1920, 'height': 1080, 'duration': 0}


# ── Progress helpers ──────────────────────────────────────────
def set_progress(batch_id, ep_idx, pct, msg, status='processing'):
    with jobs_lock:
        if batch_id not in jobs:
            jobs[batch_id] = {}
        jobs[batch_id][f'ep_{ep_idx}'] = {
            'pct': pct, 'msg': msg,
            'status': status, 'ts': time.time()}


def set_done(batch_id, results, dl_folder):
    with jobs_lock:
        jobs[batch_id]['_final'] = {
            'results':         results,
            'download_folder': dl_folder,
            'ts':              time.time()}


# ── FFmpeg helpers ────────────────────────────────────────────
def escape_path(p):
    p = p.replace('\\', '/')
    if len(p) >= 2 and p[1] == ':':
        p = p[0] + '\\:' + p[2:]
    p = p.replace("'", "\\'")
    return p


def run_ff(cmd, timeout=7200):
    print(f"[FFmpeg] {' '.join(str(c) for c in cmd)}")
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout)
        if result.returncode != 0:
            print(f"[FFmpeg STDERR] {result.stderr[-400:]}")
        else:
            print("[FFmpeg] OK")
        return result
    except subprocess.TimeoutExpired:
        print("[FFmpeg] TIMEOUT")
        return None
    except Exception as ex:
        print(f"[FFmpeg] EXCEPTION: {ex}")
        return None


def run_ff_with_progress(cmd, batch_id, ep_idx, duration,
                         start_pct=15, end_pct=90, timeout=7200):
    print(f"[FFmpeg+Progress ep{ep_idx}] "
          f"{' '.join(str(c) for c in cmd)}")
    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, bufsize=1)
        pct_range    = end_pct - start_pct
        last_pct     = start_pct
        stderr_lines = []

        for line in proc.stderr:
            stderr_lines.append(line)
            m = re.search(r'time=(\d+):(\d+):([\d.]+)', line)
            if m and duration > 0:
                elapsed = (int(m.group(1)) * 3600
                           + int(m.group(2)) * 60
                           + float(m.group(3)))
                ratio = min(elapsed / duration, 1.0)
                pct   = int(start_pct + ratio * pct_range)
                if pct > last_pct:
                    last_pct  = pct
                    mins_left = 0
                    speed_m   = re.search(r'speed=([\d.]+)x', line)
                    if speed_m and ratio > 0.01:
                        speed = float(speed_m.group(1))
                        if speed > 0:
                            remaining = (duration - elapsed) / speed
                            mins_left = int(remaining / 60)
                    msg = (f"Encoding… {int(ratio*100)}%"
                           + (f" (~{mins_left}m left)"
                              if mins_left > 0 else ""))
                    set_progress(batch_id, ep_idx, pct, msg)

        proc.wait(timeout=timeout)
        stderr_out = ''.join(stderr_lines)
        if proc.returncode != 0:
            print(f"[FFmpeg+Progress STDERR] {stderr_out[-400:]}")

        class Result:
            def __init__(self, rc, stderr):
                self.returncode = rc
                self.stderr     = stderr
                self.stdout     = ''
        return Result(proc.returncode, stderr_out)

    except subprocess.TimeoutExpired:
        proc.kill()
        print("[FFmpeg+Progress] TIMEOUT")
        return None
    except Exception as ex:
        print(f"[FFmpeg+Progress] EXCEPTION: {ex}")
        return None


def good(result, out_path):
    if not result or result.returncode != 0:
        return False
    if not os.path.exists(out_path):
        return False
    if os.path.getsize(out_path) < 10000:
        return False
    return True


# ── Hard-sub methods ──────────────────────────────────────────
def build_hard_cmd(video, vf_filter, out, encoder, enc_args):
    cmd = ['ffmpeg', '-y', '-i', os.path.abspath(video),
           '-vf', vf_filter, '-c:v', encoder]
    cmd += enc_args
    cmd += ['-c:a', 'copy', '-movflags', '+faststart',
            os.path.abspath(out)]
    return cmd


def method_ass_burn(video, sub, out, work_dir, info,
                    batch_id, ep_idx, encoder, enc_args):
    ass = os.path.join(work_dir, 'styled.ass')
    n   = create_ass(os.path.abspath(sub), ass,
                     info['width'], info['height'])
    if n == 0:
        return None
    ass_esc = escape_path(os.path.abspath(ass))
    cmd = build_hard_cmd(video, f"ass='{ass_esc}'",
                         out, encoder, enc_args)
    return run_ff_with_progress(
        cmd, batch_id, ep_idx, info['duration'])


def method_subtitles_filter(video, sub, out, sub_ext, work_dir, info,
                             batch_id, ep_idx, encoder, enc_args):
    if sub_ext == '.srt':
        clean = os.path.join(work_dir, 'clean.srt')
        clean_srt(os.path.abspath(sub), clean)
        sub_to_use = clean
    else:
        sub_to_use = sub
    abs_sub = os.path.abspath(sub_to_use)
    sub_esc = escape_path(abs_sub)
    vf = (f"ass='{sub_esc}'"
          if sub_ext in ('.ass', '.ssa')
          else f"subtitles='{sub_esc}'")
    cmd = build_hard_cmd(video, vf, out, encoder, enc_args)
    return run_ff_with_progress(
        cmd, batch_id, ep_idx, info['duration'])


def method_ffmpeg_convert_ass(video, sub, out, work_dir, info,
                               batch_id, ep_idx, encoder, enc_args):
    conv = os.path.join(work_dir, 'ffmpeg_conv.ass')
    r    = run_ff(['ffmpeg', '-y', '-i',
                   os.path.abspath(sub), conv], timeout=60)
    if not (r and r.returncode == 0 and os.path.exists(conv)):
        return None
    esc_p = escape_path(os.path.abspath(conv))
    cmd   = build_hard_cmd(video, f"ass='{esc_p}'",
                           out, encoder, enc_args)
    return run_ff_with_progress(
        cmd, batch_id, ep_idx, info['duration'])


# ── Soft-sub methods ──────────────────────────────────────────
def method_soft_mkv(video, sub, out_mkv, sub_ext):
    sub_codec = 'ass' if sub_ext in ('.ass', '.ssa') else 'srt'
    r = run_ff([
        'ffmpeg', '-y',
        '-i', os.path.abspath(video),
        '-i', os.path.abspath(sub),
        '-map', '0:v', '-map', '0:a?', '-map', '1:0',
        '-c:v', 'copy', '-c:a', 'copy', '-c:s', sub_codec,
        '-metadata:s:s:0', 'language=eng',
        '-disposition:s:0', 'default',
        out_mkv], timeout=600)
    return r, out_mkv


def method_soft_mp4(video, sub, out_mp4, work_dir):
    clean = os.path.join(work_dir, 'clean.srt')
    clean_srt(os.path.abspath(sub), clean)
    return run_ff([
        'ffmpeg', '-y',
        '-i', os.path.abspath(video),
        '-i', clean,
        '-map', '0:v', '-map', '0:a?', '-map', '1:0',
        '-c:v', 'copy', '-c:a', 'copy', '-c:s', 'mov_text',
        '-metadata:s:s:0', 'language=eng',
        '-disposition:s:0', 'default',
        '-movflags', '+faststart',
        os.path.abspath(out_mp4)], timeout=600)


# ── Core processor ────────────────────────────────────────────
def process_episode(video_path, srt_path, ep_name, merge_type,
                    batch_id, ep_idx, dl_folder):
    """
    Encodes/muxes DIRECTLY into dl_folder.
    No intermediate copy — zero extra disk usage.
    """
    work_dir = None
    out_file = None
    try:
        set_progress(batch_id, ep_idx, 2, 'Starting…')

        # ── Validate inputs ───────────────────────────────
        if not os.path.isfile(video_path):
            raise FileNotFoundError(f"Video not found: {video_path}")
        if not os.path.isfile(srt_path):
            raise FileNotFoundError(f"Subtitle not found: {srt_path}")
        if os.path.getsize(video_path) < 1000:
            raise ValueError("Video file too small")
        if os.path.getsize(srt_path) < 5:
            raise ValueError("Subtitle file too small")

        entries = parse_srt(srt_path)
        if not entries:
            raise ValueError("No subtitle entries found")

        set_progress(batch_id, ep_idx, 6,
                     f'Parsed {len(entries)} subtitles')

        info     = get_video_info(video_path)
        sub_ext  = Path(srt_path).suffix.lower()
        safe     = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', ep_name)
        safe     = (re.sub(r'_+', '_', safe).strip('_')
                    or f'episode_{ep_idx+1}')
        work_dir = tempfile.mkdtemp(prefix=f'ep{ep_idx}_')

        encoder, enc_args = get_best_encoder()
        print(f"[EP {ep_idx}] encoder={encoder}  "
              f"dur={info['duration']:.0f}s  "
              f"res={info['width']}x{info['height']}")

        os.makedirs(dl_folder, exist_ok=True)

        # ── HARD SUBTITLES — output directly to dl_folder ─
        if merge_type == 'hard':
            out_name = f'{safe}.mp4'
            out_file = _unique_dest(dl_folder, out_name)

            set_progress(batch_id, ep_idx, 10,
                         f'Encoding with {encoder}…')

            methods = [
                ('ASS burn', lambda: method_ass_burn(
                    video_path, srt_path, out_file, work_dir,
                    info, batch_id, ep_idx, encoder, enc_args)),
                ('Subtitles filter', lambda: method_subtitles_filter(
                    video_path, srt_path, out_file, sub_ext,
                    work_dir, info, batch_id, ep_idx,
                    encoder, enc_args)),
                ('FFmpeg ASS convert',
                 lambda: method_ffmpeg_convert_ass(
                    video_path, srt_path, out_file, work_dir,
                    info, batch_id, ep_idx, encoder, enc_args)),
            ]

            success  = False
            last_err = 'No methods ran'
            for mname, mfn in methods:
                print(f"[EP {ep_idx}] Trying: {mname}")
                try:
                    result = mfn()
                    if good(result, out_file):
                        print(f"[EP {ep_idx}] ✅ {mname} OK")
                        success = True
                        break
                    last_err = (result.stderr[-300:]
                                if result and result.stderr
                                else 'no output')
                    # Remove failed partial output
                    if os.path.exists(out_file):
                        os.remove(out_file)
                except Exception as ex:
                    last_err = str(ex)
                    print(f"[EP {ep_idx}] ❌ {mname}: {ex}")
                    if os.path.exists(out_file):
                        os.remove(out_file)

            if not success:
                raise RuntimeError(
                    f"All hard-sub methods failed.\n{last_err[:300]}")

        # ── SOFT SUBTITLES — output directly to dl_folder ─
        else:
            success  = False
            last_err = ''

            set_progress(batch_id, ep_idx, 15,
                         'Soft sub (stream copy)…')

            # Try MKV first
            out_mkv  = _unique_dest(dl_folder, f'{safe}.mkv')
            try:
                r, out_mkv = method_soft_mkv(
                    video_path, srt_path, out_mkv, sub_ext)
                if good(r, out_mkv):
                    out_file = out_mkv
                    success  = True
                    set_progress(batch_id, ep_idx, 90,
                                 'Stream copy done…')
                else:
                    last_err = (r.stderr[-200:]
                                if r and r.stderr else 'mkv failed')
                    if os.path.exists(out_mkv):
                        os.remove(out_mkv)
            except Exception as ex:
                last_err = str(ex)
                if os.path.exists(out_mkv):
                    os.remove(out_mkv)

            # Fallback to MP4
            if not success:
                set_progress(batch_id, ep_idx, 40,
                             'Trying MP4 soft sub…')
                out_mp4 = _unique_dest(dl_folder, f'{safe}.mp4')
                try:
                    r = method_soft_mp4(
                        video_path, srt_path, out_mp4, work_dir)
                    if good(r, out_mp4):
                        out_file = out_mp4
                        success  = True
                        set_progress(batch_id, ep_idx, 90,
                                     'Stream copy done…')
                    else:
                        last_err = (r.stderr[-200:]
                                    if r and r.stderr
                                    else 'mp4 failed')
                        if os.path.exists(out_mp4):
                            os.remove(out_mp4)
                except Exception as ex:
                    last_err = str(ex)

            if not success:
                raise RuntimeError(
                    f"Soft-sub failed: {last_err[:300]}")

        # ── Done ─────────────────────────────────────────
        size_mb = os.path.getsize(out_file) / 1024 / 1024
        set_progress(batch_id, ep_idx, 100,
                     f'✅ Done! {size_mb:.1f} MB', 'completed')

        return {
            'success':  True,
            'filename': os.path.basename(out_file),
            'path':     out_file,
            'size_mb':  round(size_mb, 1),
            'save_dir': dl_folder,
        }

    except Exception as exc:
        import traceback
        traceback.print_exc()
        msg = str(exc)
        # Clean up any partial output
        if out_file and os.path.exists(out_file):
            try:
                os.remove(out_file)
            except Exception:
                pass
        set_progress(batch_id, ep_idx, 0,
                     f'❌ {msg[:180]}', 'error')
        return {'success': False, 'error': msg, 'filename': ep_name}

    finally:
        if work_dir and os.path.isdir(work_dir):
            shutil.rmtree(work_dir, ignore_errors=True)


def _unique_dest(folder, filename):
    """Return a path that doesn't collide with existing files."""
    dest = os.path.join(folder, filename)
    if not os.path.exists(dest):
        return dest
    stem, suffix = os.path.splitext(filename)
    return os.path.join(folder,
                        f"{stem}_{uuid.uuid4().hex[:4]}{suffix}")


# ── Routes ────────────────────────────────────────────────────
@app.route('/')
def index():
    default_path = str(Path.home() / 'Downloads')
    return render_template(
        'index.html',
        ffmpeg_ok=check_ffmpeg(),
        hw=HW_ACCEL,
        default_save_path=default_path)


@app.route('/path_preset/<name>')
def path_preset(name):
    home    = Path.home()
    presets = {
        'downloads': home / 'Downloads',
        'desktop':   home / 'Desktop',
        'videos':    home / 'Videos',
        'documents': home / 'Documents',
    }
    p = presets.get(name, home / 'Downloads')
    p.mkdir(parents=True, exist_ok=True)
    return jsonify({'path': str(p)})


@app.route('/browse_folder', methods=['POST'])
def browse_folder():
    """
    Open a native OS folder-picker dialog and return the chosen path.
    Falls back gracefully if no GUI is available (headless server).
    """
    try:
        import tkinter as tk
        from tkinter import filedialog

        # Get suggested starting directory from request
        data        = request.get_json(force=True) or {}
        start_dir   = data.get('start_dir', str(Path.home()))
        start_dir   = os.path.expanduser(start_dir)
        if not os.path.isdir(start_dir):
            start_dir = str(Path.home())

        root = tk.Tk()
        root.withdraw()           # Hide the root window
        root.attributes('-topmost', True)   # Bring picker to front
        root.update()

        chosen = filedialog.askdirectory(
            parent=root,
            title='Choose output folder',
            initialdir=start_dir,
            mustexist=False)

        root.destroy()

        if not chosen:
            return jsonify({'cancelled': True})

        # Validate / create chosen folder
        norm, err = validate_save_path(chosen)
        if err:
            return jsonify({'error': err})
        return jsonify({'path': norm})

    except ImportError:
        return jsonify({
            'error': 'tkinter not available on this server. '
                     'Please type the path manually.'})
    except Exception as ex:
        return jsonify({'error': f'Folder picker error: {ex}'})


@app.route('/debug')
def debug():
    enc, enc_args = get_best_encoder()
    info = {
        'ffmpeg':       check_ffmpeg(),
        'hw_accel':     HW_ACCEL,
        'best_encoder': enc,
        'cpu_cores':    CPU_CORES,
        'ff_threads':   FF_THREADS,
        'max_episodes': MAX_EPISODES,
        'home':         str(Path.home()),
    }
    try:
        r = subprocess.run(['ffmpeg', '-version'],
                           capture_output=True, text=True)
        info['ffmpeg_version'] = r.stdout.split('\n')[0]
    except Exception as ex:
        info['ffmpeg_version'] = str(ex)
    return jsonify(info)


@app.route('/validate_path', methods=['POST'])
def validate_path_route():
    data     = request.get_json(force=True)
    raw_path = data.get('path', '')
    norm, err = validate_save_path(raw_path)
    if err:
        return jsonify({'valid': False, 'error': err})
    return jsonify({'valid': True, 'resolved': norm, 'writable': True})


@app.route('/merge', methods=['POST'])
def merge():
    try:
        if not check_ffmpeg():
            return jsonify({'error': 'FFmpeg not installed'}), 500

        merge_type = request.form.get('merge_type', 'hard')
        ep_count   = int(request.form.get('episode_count', 0))
        save_path  = request.form.get('save_path', '').strip()

        print(f"\n[/merge] type={merge_type} "
              f"ep_count={ep_count} save_path={save_path!r}")

        if ep_count == 0:
            return jsonify({'error': 'No episodes provided'}), 400
        if ep_count > MAX_EPISODES:
            return jsonify(
                {'error': f'Maximum {MAX_EPISODES} episodes'}), 400

        dl_folder, path_err = validate_save_path(save_path)
        if path_err:
            return jsonify(
                {'error': f'Invalid save path: {path_err}'}), 400

        batch_id   = uuid.uuid4().hex[:8]
        # Temp dir only for uploaded source files — deleted after processing
        upload_dir = tempfile.mkdtemp(prefix=f'upload_{batch_id}_')

        episodes = []
        for i in range(ep_count):
            vf = request.files.get(f'video_{i}')
            sf = request.files.get(f'srt_{i}')
            nm = request.form.get(
                f'ep_name_{i}', f'Episode_{i+1}').strip()

            if (not vf or not vf.filename
                    or not sf or not sf.filename):
                continue

            v_ext  = Path(vf.filename).suffix.lower() or '.mp4'
            s_ext  = Path(sf.filename).suffix.lower() or '.srt'
            v_path = os.path.join(upload_dir, f'video_{i}{v_ext}')
            s_path = os.path.join(upload_dir, f'srt_{i}{s_ext}')
            vf.save(v_path)
            sf.save(s_path)

            print(f"  ep {i:02d}: {nm!r} "
                  f"video={os.path.getsize(v_path):,}b "
                  f"srt={os.path.getsize(s_path):,}b")

            episodes.append({
                'video': v_path,
                'srt':   s_path,
                'name':  nm or f'Episode_{i+1}'})

        if not episodes:
            shutil.rmtree(upload_dir, ignore_errors=True)
            return jsonify(
                {'error': 'No valid episodes'}), 400

        with jobs_lock:
            jobs[batch_id] = {
                f'ep_{i}': {
                    'pct': 0, 'msg': 'Queued…',
                    'status': 'queued', 'ts': time.time()}
                for i in range(len(episodes))}
            jobs[batch_id]['_meta'] = {
                'save_path': dl_folder,
                'ep_count':  len(episodes)}

        def run_batch():
            results = []
            for i, ep in enumerate(episodes):
                r = process_episode(
                    ep['video'], ep['srt'], ep['name'],
                    merge_type, batch_id, i, dl_folder)
                results.append(r)
            set_done(batch_id, results, dl_folder)
            # Delete uploaded source files — output already in dl_folder
            shutil.rmtree(upload_dir, ignore_errors=True)
            print(f"[Batch {batch_id}] Cleaned upload temp dir")

        threading.Thread(target=run_batch, daemon=True).start()

        return jsonify({
            'batch_id':  batch_id,
            'ep_count':  len(episodes),
            'save_path': dl_folder})

    except Exception as ex:
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(ex)}), 500


@app.route('/progress/<batch_id>')
def progress_poll(batch_id):
    with jobs_lock:
        return jsonify(jobs.get(batch_id, {}))


@app.route('/progress_stream/<batch_id>')
def progress_stream(batch_id):
    def gen():
        last    = None
        elapsed = 0
        timeout = MAX_EPISODES * 7200
        while elapsed < timeout:
            with jobs_lock:
                data = dict(jobs.get(batch_id, {}))
            s = json.dumps(data)
            if s != last:
                yield f"data: {s}\n\n"
                last = s
                if '_final' in data:
                    break
            time.sleep(0.5)
            elapsed += 0.5
        yield 'data: {"_done":true}\n\n'

    return Response(
        gen(),
        mimetype='text/event-stream',
        headers={
            'Cache-Control':    'no-cache',
            'X-Accel-Buffering': 'no',
            'Connection':       'keep-alive'})


# NOTE: /download route kept for compatibility but files are already
# saved directly to the user's chosen folder — no server copy exists.
@app.route('/download/<batch_id>/<int:ep_idx>')
def download(batch_id, ep_idx):
    with jobs_lock:
        fin     = jobs.get(batch_id, {}).get('_final', {})
        results = fin.get('results', [])
    if ep_idx < len(results) and results[ep_idx].get('success'):
        path = results[ep_idx].get('path', '')
        if os.path.isfile(path):
            fname = os.path.basename(path)
            ext   = Path(path).suffix.lower()
            mime  = ('video/x-matroska'
                     if ext == '.mkv' else 'video/mp4')
            return send_file(os.path.abspath(path),
                             as_attachment=True,
                             download_name=fname,
                             mimetype=mime)
    return jsonify({'error': 'File not found — '
                             'check your output folder directly'}), 404


@app.errorhandler(413)
def too_large(e):
    return jsonify({'error': 'File too large'}), 413

@app.errorhandler(500)
def server_error(e):
    return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    print("=" * 60)
    print("  🎬  VIDEO + SUBTITLE MERGER")
    print("=" * 60)
    ok = check_ffmpeg()
    print(f"  FFmpeg    : {'✅ found' if ok else '❌ NOT FOUND'}")
    if ok:
        enc, _ = get_best_encoder()
        print(f"  Encoder   : {enc}")
        for k, v in HW_ACCEL.items():
            if v: print(f"  HW Accel  : ✅ {k}")
    print(f"  CPU cores : {CPU_CORES}  → FF threads: {FF_THREADS}")
    print(f"  Max eps   : {MAX_EPISODES}")
    print(f"  URL       : http://localhost:{port}")
    print("  Output    : Saved DIRECTLY to chosen folder (no copy)")
    print("=" * 60)
    app.run(debug=False, host='0.0.0.0', port=port, threaded=True)