import argparse

from .jobman import JobMan

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('command')
    parsed_args, command_args = parser.parse_known_args()
    if parsed_args.command == 'flush':
        jobman = JobMan()
        jobman.flush()
