import argparse

parser = argparse.ArgumentParser(description="Example argument parser")
parser.add_argument('--name', type=str, required=True, help='Your name', default='User')
parser.add_argument('--age', type=int, help='Your age', default=18)

args = parser.parse_args()
print(args.name)