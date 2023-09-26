#!/usr/bin/env python3
import getopt
import logging
import os
import sys
from datetime import datetime
from typing import Union

import dropbox
from dropbox.exceptions import ApiError

logger = logging.getLogger(__name__)


KILOBYTES_MULT = 1024
MEGABYTES_MULT = 1024 * 1024
GIGABYTES_MULT = 1024 * 1024 * 1024


def bytes_to_human_str(byte_count: int) -> str:
    if byte_count > GIGABYTES_MULT:
        return "{:.2f}G".format(float(byte_count) / GIGABYTES_MULT)
    if byte_count > MEGABYTES_MULT:
        return "{:.2f}M".format(float(byte_count) / MEGABYTES_MULT)
    if byte_count > KILOBYTES_MULT:
        return "{:.2f}M".format(float(byte_count) / KILOBYTES_MULT)
    return str(byte_count)


def replace_extension(path, new_extension):
    if path is None:
        return None
    return os.path.splitext(path)[0] + '.' + new_extension


def edl_for_video(filepath):
    edl_path = replace_extension(filepath, 'edl')
    if os.path.exists(edl_path):
        return edl_path
    edl_bak_path = replace_extension(filepath, 'bak.edl')
    if os.path.exists(edl_bak_path):
        return edl_bak_path
    return edl_path


VIDEO_EXTENSIONS = ["mkv", "ts", "mp4", "mov"]


def is_video_file(filepath) -> bool:
    if os.path.basename(filepath).startswith('._'):
        # Apple metainfo
        return False
    return len(list(filter(lambda e: filepath.endswith('.' + e), VIDEO_EXTENSIONS))) > 0


def find_precut_revision(dbx, path, has_been_cut: bool, no_older_than_time: Union[datetime, None]):
    entries = dbx.files_list_revisions(path, limit=30).entries
    revisions = sorted(entries, key=lambda entry: entry.server_modified, reverse=True)
    latest_size = revisions[0].size
    logger.info("%s: latest_size = %s, has been cut = %s", path, bytes_to_human_str(latest_size),
                str(has_been_cut))
    if has_been_cut:
        target_size = latest_size * 1.07
    else:
        target_size = latest_size * 1.1
    stop_size_sm = latest_size * 0.8
    stop_size_lg = latest_size * 1.8
    sizes = list()
    for rev in revisions[1:]:
        if no_older_than_time and rev.server_modified < no_older_than_time:
            break
        size_str = bytes_to_human_str(rev.size)
        if len(sizes) == 0 or sizes[-1] != size_str:
            sizes.append(size_str)

        if rev.size > stop_size_lg:
            logger.info("%s: found pre-transcoded version, %s bytes at %s", path, size_str, rev.server_modified)
            return None
        if rev.size > target_size:
            logger.info("%s: found pre-cut version, %s bytes at %s", path, size_str, rev.server_modified)
            return rev
        if rev.size < stop_size_sm:
            logger.info("%s: found cut version, %s bytes, looks like current version is pre-cut", path, size_str)
            return None

    logger.info("%s: no pre-cut version found from sizes %s", path, sizes)
    return None


def generate_files(media_paths: list[str]):
    for media_path in media_paths:
        if os.path.isfile(media_path):
            if is_video_file(media_path):
                yield os.path.abspath(media_path)
        for root, dirs, files in os.walk(media_path):
            files.sort()
            for file in files:
                filepath = os.path.abspath(os.path.join(root, file))
                filename = os.path.basename(filepath)
                if not filename.startswith('.') and is_video_file(filepath):
                    yield filepath


def comcut_restore_cli(argv):
    dry_run = False
    media_base = None
    no_older_than_time: Union[datetime, None] = None

    try:
        opts, args = getopt.getopt(list(argv),
                                   "hnvd:t:",
                                   ["help", "verbose", "dry-run", "dir=", "lt="])
    except getopt.GetoptError:
        usage()
        return 255
    for opt, arg in opts:
        if opt in ['-h', '--help']:
            usage()
            return 255
        elif opt == "--verbose":
            logging.getLogger().setLevel(logging.DEBUG)
        elif opt in ["-n", "--dry-run"]:
            dry_run = True
        elif opt in ["-d", "--dir"]:
            media_base = arg
            if media_base.endswith("/"):
                media_base = media_base[:-1]
        elif opt in ["--lt"]:
            no_older_than_time = datetime.fromisoformat(arg)

    if len(args) == 0:
        sys.exit("ERROR: Expected list of files or directories")

    if not media_base:
        sys.exit("ERROR: --dir is required")

    token = os.getenv('DROPBOX_ACCESS_TOKEN')
    if not token:
        sys.exit("ERROR: Access token required in DROPBOX_ACCESS_TOKEN")

    with dropbox.Dropbox(token) as dbx:
        for video_filepath in generate_files(args):
            edl_filepath = edl_for_video(video_filepath)
            has_been_cut = False
            if edl_filepath and os.path.isfile(edl_filepath):
                with open(edl_filepath, 'r') as f:
                    has_been_cut = '## cut complete' in f.read()
            dropbox_path = video_filepath.replace(media_base, '/Media')
            logger.info(dropbox_path)
            uncut_rev = find_precut_revision(dbx, dropbox_path, has_been_cut, no_older_than_time)
            if uncut_rev is not None:
                logger.info("Restoring %s, size %s", dropbox_path, bytes_to_human_str(uncut_rev.size))
                if not dry_run:
                    try:
                        dbx.files_restore(dropbox_path, uncut_rev.rev)
                        try:
                            dbx.files_delete_v2(replace_extension(dropbox_path, 'edl'))
                        except ApiError:
                            pass
                        try:
                            dbx.files_delete_v2(replace_extension(dropbox_path, 'bak.edl'))
                        except ApiError:
                            pass
                    except ApiError as e:
                        logger.error(f"Restoring {dropbox_path} revision {uncut_rev.rev}, {repr(e)}")

    return 0


def usage():
    print(f"""
Restore previous uncut version of files. Looks at video files with at least 15% cut in prior version.

Usage: {sys.argv[0]} file | dir

--verbose
-n, --dry-run
-d, --dir
    media base directory
--lt yyyy-mm-ddTmm:hh:ss
    versions are no older than
""", file=sys.stderr)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s:%(lineno)d %(message)s', level=logging.INFO)
    sys.exit(comcut_restore_cli(sys.argv[1:]))
