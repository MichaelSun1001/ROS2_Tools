import os
import cv2
import numpy as np
from sensor_msgs.msg import Image, CompressedImage
from rosbag2_py import SequentialReader, StorageOptions, ConverterOptions
from rclpy.serialization import deserialize_message
from rosbag2_py import TopicMetadata

parent_dir = "/media/sax/新加卷/db3"
output_parent_dir = "/media/sax/新加卷/processed_images"

os.makedirs(output_parent_dir, exist_ok=True)


def process_db3_file(db3_file, output_dir):
    """处理单个 DB3 文件，提取并保存图像数据"""
    if not os.path.isfile(db3_file):
        print(f"Error: DB3 file '{db3_file}' not found.")
        return

    # 直接使用 db3 文件的基础名称创建输出目录
    db3_base_name = os.path.splitext(os.path.basename(db3_file))[0]
    output_image_dir = output_dir  # 使用上层传入的输出目录

    # 确保输出目录存在
    os.makedirs(output_image_dir, exist_ok=True)

    # 设置存储选项
    storage_options = StorageOptions(uri=db3_file, storage_id="sqlite3")
    converter_options = ConverterOptions("", "")
    reader = SequentialReader()
    reader.open(storage_options, converter_options)

    print(f"开始处理 db3 文件 '{db3_file}' 中的图像数据...")

    # 为每个 topic 初始化独立的 frame_id_image
    topic_frame_counters = {}

    # 读取话题信息，获取所有的topic类型
    topics_metadata = reader.get_all_topics_and_types()
    topic_types = {topic.name: topic.type for topic in topics_metadata}

    while reader.has_next():
        topic, serialized_msg, timestamp_ns = reader.read_next()
        print(f"Reading topic: {topic}")

        # 判断消息类型并反序列化
        msg_type = topic_types.get(topic)
        if not msg_type:
            print(f"Warning: Unknown message type for topic '{topic}'")
            continue

        try:
            if msg_type == "sensor_msgs/msg/CompressedImage":  # 处理压缩图像消息
                msg = deserialize_message(serialized_msg, CompressedImage)
                image_data = np.frombuffer(msg.data, dtype=np.uint8)
                image = cv2.imdecode(image_data, cv2.IMREAD_COLOR)
                if image is not None:
                    secs = msg.header.stamp.sec
                    nsecs = msg.header.stamp.nanosec
                    timestamp = f"{secs}.{nsecs:09d}"  # 完整时间戳
                    frame_id_image = topic_frame_counters.get(topic, 0)
                    topic_frame_counters[topic] = frame_id_image + 1
                    image_file_path = os.path.join(
                        output_image_dir,
                        f"{topic.replace('/', '_').strip('_')}_compressed_{frame_id_image}_{timestamp}.png",
                    )
                    cv2.imwrite(image_file_path, image)
                    print(f"Compressed image saved to {image_file_path}")
                else:
                    print("Failed to decode compressed image")
            elif msg_type == "sensor_msgs/msg/Image":  # 处理普通图像消息
                msg = deserialize_message(serialized_msg, Image)
                secs = msg.header.stamp.sec
                nsecs = msg.header.stamp.nanosec
                timestamp = f"{secs}.{nsecs}"  # 完整时间戳
                image_data = np.frombuffer(msg.data, dtype=np.uint8).reshape(
                    msg.height, msg.width, -1
                )
                frame_id_image = topic_frame_counters.get(topic, 0)
                topic_frame_counters[topic] = frame_id_image + 1
                image_file_path = os.path.join(
                    output_image_dir,
                    f"{topic.replace('/', '_').strip('_')}_raw_{frame_id_image}_{timestamp}.png",
                )
                cv2.imwrite(image_file_path, image_data)
                print(f"Image saved to {image_file_path}")
            else:
                print(f"Unsupported message type for topic '{topic}'")

        except Exception as e:
            print(f"Error processing message from topic '{topic}': {e}")
            continue


def process_all_db3_files(parent_dir, output_parent_dir):
    """处理所有 DB3 文件"""
    for root, dirs, files in os.walk(parent_dir):
        for file in files:
            if file.endswith(".db3"):
                db3_file_path = os.path.join(root, file)

                # 直接使用 db3 文件所在目录的名字作为输出目录
                relative_path = os.path.relpath(root, parent_dir)
                output_dir = os.path.join(output_parent_dir, relative_path)

                os.makedirs(output_dir, exist_ok=True)

                print(f"开始处理 {db3_file_path} ...")
                process_db3_file(db3_file_path, output_dir)


# 示例：调用函数来处理所有 db3 文件
process_all_db3_files(parent_dir, output_parent_dir)
