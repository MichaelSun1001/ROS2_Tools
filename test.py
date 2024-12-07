import os
import cv2
import numpy as np
from sensor_msgs.msg import Image, CompressedImage
from rosbag2_py import SequentialReader, StorageOptions, ConverterOptions
from rclpy.serialization import deserialize_message
from rosbag2_py import TopicMetadata

parent_dir = "/media/sax/新加卷/db3"
output_parent_dir = "/media/sax/新加卷/processed_images"

# # 注意，如果想指定话题进行读取，请开启这部分代码。
# # 需要处理的指定话题列表
# topics_to_process = [
#     "/usb_cam_1/compressed",  # 话题1
#     "/camera/depth/image_raw",  # 话题2
#     "/hugin_raf_1/radar_data",
#     # 添加其他你需要处理的话题
# ]

os.makedirs(output_parent_dir, exist_ok=True)


def process_db3_file(db3_file, output_dir):
    """处理单个 DB3 文件，提取并保存图像数据"""
    if not os.path.isfile(db3_file):
        print(f"Error: DB3 file '{db3_file}' not found.")
        return

    # 使用 DB3 文件的基础名称作为参考
    db3_base_name = os.path.splitext(os.path.basename(db3_file))[0]
    output_image_dir = output_dir  # 上层传入的输出目录

    # 确保输出目录存在
    os.makedirs(output_image_dir, exist_ok=True)

    # 设置存储选项
    storage_options = StorageOptions(uri=db3_file, storage_id="sqlite3")
    converter_options = ConverterOptions("", "")
    reader = SequentialReader()
    reader.open(storage_options, converter_options)

    print(f"开始处理 DB3 文件 '{db3_file}' 中的图像数据...")

    # 为每个话题初始化独立的 frame_id_image
    topic_frame_counters = {}

    # 获取所有话题的元数据
    topics_metadata = reader.get_all_topics_and_types()
    topic_types = {topic.name: topic.type for topic in topics_metadata}

    while reader.has_next():
        topic, serialized_msg, timestamp_ns = reader.read_next()

        # # 注意，如果想指定话题进行读取，请开启这部分代码。
        # # 判断当前话题是否在需要处理的列表中
        # if topic not in topics_to_process:
        #     print(f"No topic in the list !!! ")
        #     print(f"No topic in the list !!! ")
        #     print(f"No topic in the list !!! ")
        #     return  # 如果当前话题不在指定列表中，跳过此话题

        # 获取话题类型
        msg_type = topic_types.get(topic)
        if not msg_type:
            print(f"Warning: Unknown message type for topic '{topic}'")
            continue

        # 检查是否为图像消息类型
        if msg_type not in ["sensor_msgs/msg/CompressedImage", "sensor_msgs/msg/Image"]:
            print(f"Skipping non-image topic '{topic}'")
            continue
        else:
            print(f"Processing topic: {topic}")

        # 为图像话题创建对应的输出目录
        topic_name = topic.replace("/", "_").strip("_")
        topic_output_dir = os.path.join(output_image_dir, topic_name)
        os.makedirs(topic_output_dir, exist_ok=True)

        try:
            if msg_type == "sensor_msgs/msg/CompressedImage":  # 处理压缩图像
                msg = deserialize_message(serialized_msg, CompressedImage)

                # 注意，这里是按照8位深来读取的，如果是16位深请注意修改。
                # image_data = np.frombuffer(msg.data, dtype=np.uint16)
                image_data = np.frombuffer(msg.data, dtype=np.uint8)

                # 解码图像数据
                if msg.format == "jpeg":
                    image = cv2.imdecode(image_data, cv2.IMREAD_COLOR)
                elif msg.format == "png":
                    image = cv2.imdecode(image_data, cv2.IMREAD_UNCHANGED)
                else:
                    print(f"Unsupported compressed image format: {msg.format}")
                    continue

                # 如果是16位深，请开启这里的归一化处理
                if image is not None:

                    # # 如果是16位图像，请开启这段代码，进行归一化处理。
                    # if image.dtype == np.uint16:
                    #     # 归一化为 8 位
                    #     image = cv2.normalize(image, None, 0, 255, cv2.NORM_MINMAX)
                    #     image = np.uint8(image)

                    # 获取时间戳和帧 ID
                    secs, nsecs = msg.header.stamp.sec, msg.header.stamp.nanosec
                    timestamp = f"{secs}.{nsecs:09d}"
                    frame_id_image = topic_frame_counters.get(topic, 0)
                    topic_frame_counters[topic] = frame_id_image + 1

                    # 根据通道数保存图像
                    if len(image.shape) == 2:  # 灰度图
                        image_file_path = os.path.join(
                            topic_output_dir,
                            f"CompressedImage_gray_{frame_id_image}_{timestamp}.png",
                        )
                    elif image.shape[2] == 3:  # RGB 图像
                        image_file_path = os.path.join(
                            topic_output_dir,
                            f"CompressedImage_rgb_{frame_id_image}_{timestamp}.png",
                        )
                    elif image.shape[2] == 4:  # RGBA 图像
                        image_file_path = os.path.join(
                            topic_output_dir,
                            f"CompressedImage_rgba_{frame_id_image}_{timestamp}.png",
                        )
                    else:
                        print("Unsupported channel configuration")
                        continue

                    cv2.imwrite(image_file_path, image)
                    print(f"Compressed image saved to {image_file_path}")

            elif msg_type == "sensor_msgs/msg/Image":  # 处理普通图像
                msg = deserialize_message(serialized_msg, Image)
                secs, nsecs = msg.header.stamp.sec, msg.header.stamp.nanosec
                timestamp = f"{secs}.{nsecs:09d}"

                # 获取编码方式
                encoding = msg.encoding

                # 根据不同的编码方式处理图像数据
                if encoding == "rgb8":
                    # RGB 8-bit 图像
                    image_data = np.frombuffer(msg.data, dtype=np.uint8).reshape(
                        msg.height, msg.width, 3
                    )
                elif encoding == "bgr8":
                    # BGR 8-bit 图像
                    image_data = np.frombuffer(msg.data, dtype=np.uint8).reshape(
                        msg.height, msg.width, 3
                    )
                elif encoding == "mono8":
                    # 单通道 8-bit 图像（灰度图）
                    image_data = np.frombuffer(msg.data, dtype=np.uint8).reshape(
                        msg.height, msg.width
                    )
                elif encoding == "rgba8":
                    # RGBA 8-bit 图像（含透明通道）
                    image_data = np.frombuffer(msg.data, dtype=np.uint8).reshape(
                        msg.height, msg.width, 4
                    )
                elif encoding == "bgra8":
                    # BGRA 8-bit 图像（含透明通道）
                    image_data = np.frombuffer(msg.data, dtype=np.uint8).reshape(
                        msg.height, msg.width, 4
                    )
                elif encoding == "mono16":
                    # 单通道 16-bit 图像（灰度图）
                    image_data = np.frombuffer(msg.data, dtype=np.uint16).reshape(
                        msg.height, msg.width
                    )
                elif encoding == "rgba16":
                    # RGBA 16-bit 图像（含透明通道）
                    image_data = np.frombuffer(msg.data, dtype=np.uint16).reshape(
                        msg.height, msg.width, 4
                    )
                elif encoding == "bgra16":
                    # BGRA 16-bit 图像（含透明通道）
                    image_data = np.frombuffer(msg.data, dtype=np.uint16).reshape(
                        msg.height, msg.width, 4
                    )
                else:
                    # 不支持的编码方式
                    raise ValueError(f"Unsupported image encoding: {encoding}")

                # 获取帧 ID 并保存文件
                frame_id_image = topic_frame_counters.get(topic, 0)
                topic_frame_counters[topic] = frame_id_image + 1
                image_file_path = os.path.join(
                    topic_output_dir,
                    f"Image_{frame_id_image}_{timestamp}.png",
                )

                # 如果是 16 位图像，可能需要进行归一化或调整为 8 位图像保存
                if encoding in ["mono16", "rgba16", "bgra16"]:
                    # 归一化为 8 位
                    image_data = cv2.normalize(
                        image_data, None, 0, 255, cv2.NORM_MINMAX
                    )
                    image_data = np.uint8(image_data)

                # 保存图像
                cv2.imwrite(image_file_path, image_data)
                print(f"Image saved to {image_file_path}")

        except Exception as e:
            print(f"Error processing topic '{topic}': {e}")
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
