import argparse


def main():
    parser = argparse.ArgumentParser(description='Example script with a boolean parameter')

    # Adding a boolean argument with default value False
    parser.add_argument('--verbose', action='store_true', default=False, help='Enable verbose mode')

    args = parser.parse_args()

    if args.verbose:
        print("Verbose mode enabled")
    else:
        print("Verbose mode disabled")


if __name__ == "__main__":
    main()
