import sys
import datetime

from src.disk_delta import DiskDelta

def main():
    if len(sys.argv) < 3:
        print("Please provide two file paths as arguments.")
        return

    initial_image_path = sys.argv[1]
    target_image_path = sys.argv[2]
    
    disk_delta = DiskDelta.generate(initial_image_path, target_image_path)

    current_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    output_path = f"output/diskdelta_{current_datetime}"

    with open(output_path, "w") as f:
        f.write(disk_delta)

if __name__ == "__main__":
    main()