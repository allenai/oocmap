import re

version_line_re = re.compile(r"VERSION *= *'(.*)'")


def parse():
    version = None
    with open('setup.py', "rt") as f:
        for line in f:
            line = line.strip()
            m = version_line_re.match(line)
            if m is not None:
                version = m.group(1)
                break
    if version is None:
        raise RuntimeError('Could not find version number in setup.py.')
    return version


if __name__ == "__main__":
    print(parse())
