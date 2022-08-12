from datetime import datetime
from pathlib import Path

import version


def main():
    current_version = version.parse()

    changelog = Path("CHANGELOG.md")
    with changelog.open() as f:
        lines = f.readlines()

    insert_index: int
    for i in range(len(lines)):
        line = lines[i]
        if line.startswith("## Unreleased"):
            insert_index = i + 1
        elif line.startswith(f"## [v{current_version}]"):
            print("CHANGELOG already up-to-date")
            return
        elif line.startswith("## [v"):
            break
    else:
        raise RuntimeError("Couldn't find 'Unreleased' section")

    lines.insert(insert_index, "\n")
    lines.insert(
        insert_index + 1,
        f"## [v{current_version}](https://github.com/allenai/oocmap/releases/tag/v{current_version}) - "
        f"{datetime.now().strftime('%Y-%m-%d')}\n",
    )

    with changelog.open("w") as f:
        f.writelines(lines)


if __name__ == "__main__":
    main()
