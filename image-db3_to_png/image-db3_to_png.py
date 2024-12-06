import os
import cv2
import numpy as np
from sensor_msgs.msg import Image, CompressedImage
from rosbag2_py import SequentialReader, StorageOptions, ConverterOptions
from rclpy.serialization import deserialize_message

# 定义要读取的 topic 和对应的消息类型
topics_to_check = {
    "/usb_cam_1/compressed": CompressedImage,
    "/camera/depth/image_raw": Image,
}

parent_dir = "/media/sax/新加卷/db3"
output_parent_dir = "/media/sax/新加卷/processed_images"

os.makedirs(output_parent_dir, exist_ok=True)


def process_db3_file(db3_file, output_dir):
    if not os.path.isfile(db3_file):
        print(f"Error: DB3 file '{db3_file}' not found.")
        return

    db3_base_name = os.path.splitext(os.path.basename(db3_file))[0]
    output_image_dir = os.path.join(output_dir, db3_base_name)
    os.makedirs(output_image_dir, exist_ok=True)

    # 设置存储选项
    storage_options = StorageOptions(uri=db3_file, storage_id="sqlite3")
    converter_options = ConverterOptions("", "")
    reader = SequentialReader()
    reader.open(storage_options, converter_options)

    print(f"开始处理 db3 文件 '{db3_file}' 中的图像数据...")

    frame_id_image = 0
    while reader.has_next():
        topic, serialized_msg, timestamp_ns = reader.read_next()
        print(f"Reading topic: {topic}")

        if topic not in topics_to_check:
            continue

        try:
            if topic == "/usb_cam_1/compressed":
                msg = deserialize_message(serialized_msg, CompressedImage)
                image_data = np.frombuffer(msg.data, dtype=np.uint8)
                image = cv2.imdecode(image_data, cv2.IMREAD_COLOR)
                if image is not None:
                    timestamp = f"{msg.header.stamp.sec}.{msg.header.stamp.nanosec:09d}"
                    image_file_path = os.path.join(
                        output_image_dir,
                        f"image_{frame_id_image:04d}_{timestamp}.png",
                    )
                    cv2.imwrite(image_file_path, image)
                    print(f"Compressed image saved to {image_file_path}")
                else:
                    print("Failed to decode compressed image")
            else:  # 处理普通 Image 消息
                msg = deserialize_message(serialized_msg, Image)
                timestamp = f"{msg.header.stamp.sec}.{msg.header.stamp.nanosec:09d}"
                image_data = np.frombuffer(msg.data, dtype=np.uint8).reshape(
                    msg.height, msg.width, -1
                )
                image_file_path = os.path.join(
                    output_image_dir, f"image_{frame_id_image:04d}_{timestamp}.png"
                )
                cv2.imwrite(image_file_path, image_data)
                print(f"Image saved to {image_file_path}")
            frame_id_image += 1
        except Exception as e:
            print(f"Error processing message from topic '{topic}': {e}")
            continue

    # 删除这行代码，因为 reader 会在文件结束时自动停止
    # reader.reset()  # 结束文件读取过程


def process_all_db3_files(parent_dir, output_parent_dir):
    # 遍历父目录下的所有子文件夹
    for root, dirs, files in os.walk(parent_dir):
        # 只处理 db3 文件
        for file in files:
            if file.endswith(".db3"):
                db3_file_path = os.path.join(root, file)

                # 创建一个与 db3 文件所在文件夹同名的输出文件夹
                relative_path = os.path.relpath(root, parent_dir)
                output_dir = os.path.join(output_parent_dir, relative_path)

                # 确保输出文件夹存在
                os.makedirs(output_dir, exist_ok=True)

                print(f"开始处理 {db3_file_path} ...")
                process_db3_file(db3_file_path, output_dir)


# 示例：调用函数来处理所有 db3 文件
process_all_db3_files(parent_dir, output_parent_dir)
