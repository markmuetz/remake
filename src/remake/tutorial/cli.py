"""`remake-tutorial`: set up and drive the interactive tutorial workspace.

  remake-tutorial init DIR     create the workspace (git repo + lesson tags)
  remake-tutorial reset N      restore lesson N's starting state
  remake-tutorial watch        one line per finished remake command (tutor)
  remake-tutorial lesson N     lesson N's spec as JSON (tutor)
  remake-tutorial log          every command so far, summarised (tutor)
"""
import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

from . import watch, workspace
from .lessons import lesson


def main(argv=None):
    p = argparse.ArgumentParser(prog='remake-tutorial', description=__doc__.split('\n')[0])
    sub = p.add_subparsers(dest='cmd', required=True)
    sub.add_parser('init').add_argument('dir')
    sub.add_parser('reset').add_argument('lesson', type=int)
    sub.add_parser('watch')
    sub.add_parser('lesson').add_argument('lesson', type=int)
    sub.add_parser('log')
    args = p.parse_args(argv)

    if args.cmd == 'init':
        ws = workspace.init(args.dir)
        print(f'Tutorial workspace ready: {ws}\n\n'
              f'  cd {ws}\n\n'
              'Then follow lesson 1 of the tutorial in the remake docs. For '
              'feedback as you go,\nstart Claude Code in a second terminal in '
              'the same directory and type /remake-tutor.')
        return 0
    if args.cmd == 'lesson':
        print(json.dumps(asdict(lesson(args.lesson)), indent=1))
        return 0
    if not Path('.tutorial').is_dir():
        sys.exit(f'remake-tutorial {args.cmd}: run it from the tutorial workspace')
    if args.cmd == 'reset':
        kept = workspace.reset('.', args.lesson)
        for item in kept:
            print(f'Kept {item}')
        print(f'Workspace reset to the start of lesson {args.lesson}.')
    elif args.cmd == 'watch':
        watch.main()
    elif args.cmd == 'log':
        if watch.LOG.exists():
            for _, s in watch.read_invocations():
                print(watch.format_summary(s))
    return 0


if __name__ == '__main__':
    sys.exit(main())
