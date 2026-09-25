"""`remake-tutorial`: set up and drive the interactive tutorial workspace.

  remake-tutorial init DIR     create the workspace (git repo + lesson tags)
  remake-tutorial reset N      go to the start of lesson N
  remake-tutorial watch        one line per finished command (tutor)
  remake-tutorial spec N       lesson N's spec as JSON (tutor)
  remake-tutorial log          every command so far, summarised (tutor)
"""
import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

from . import watch, workspace
from .lessons import lesson

DOCS = 'https://markmuetz.github.io/remake'


def main(argv=None):
    p = argparse.ArgumentParser(prog='remake-tutorial', description=__doc__.split('\n')[0])
    sub = p.add_subparsers(dest='cmd', required=True)
    sub.add_parser('init').add_argument('dir')
    sub.add_parser('reset').add_argument('lesson', type=int)
    sub.add_parser('watch')
    sub.add_parser('spec').add_argument('lesson', type=int)
    sub.add_parser('log')
    args = p.parse_args(argv)

    if args.cmd == 'init':
        ws = workspace.init(args.dir)
        print(f'Tutorial workspace ready: {ws}\n\n'
              f'  cd {ws}\n\n'
              f'Then follow the tutorial: {DOCS}/tutorial/\n'
              'It starts with: remake-tutorial reset 1\n\n'
              'For a tutor as you go, start Claude Code in a second terminal in the '
              'same directory\nand type /remake-tutor before you begin.')
        return 0
    if args.cmd == 'spec':
        print(json.dumps(asdict(lesson(args.lesson)), indent=1))
        return 0
    if not Path('.tutorial').is_dir():
        sys.exit(f'remake-tutorial {args.cmd}: run it from the tutorial workspace')
    if args.cmd == 'reset':
        lesson(args.lesson)  # a clean error for an unknown lesson, before anything moves
        workspace.emit('.', 'reset_started', lesson=args.lesson)
        try:
            kept = workspace.reset('.', args.lesson)
        except BaseException as e:
            try:
                workspace.emit('.', 'reset', lesson=args.lesson, status='failed',
                               error=str(e) or type(e).__name__)
            except Exception:  # noqa: BLE001 — never mask the reset's own error
                pass
            raise
        workspace.emit('.', 'reset', lesson=args.lesson, status='ready', kept=kept)
        for item in kept:
            print(f'Kept {item}')
        les = lesson(args.lesson)
        print(f'Ready for lesson {les.number}: {les.title}.\n'
              f'  {DOCS}/tutorial/lesson-{les.number}/')
    elif args.cmd == 'watch':
        watch.main()
    elif args.cmd == 'log':
        for line in watch.history():
            print(line)
    return 0


if __name__ == '__main__':
    sys.exit(main())
