from collections import defaultdict
from glob import glob
from pprint import pprint
import random

import numpy as np
from PIL import Image
from mrjob.job import MRJob
from mrjob.step import MRStep


K = 10
IMAGES_DIR = 'images/'
MAX_ITERATIONS_NUMBER = 30
MIN_ITERATIONS_NUMBER = 5


def read_image(image_path):
    with Image.open(IMAGES_DIR + image_path) as image:
        return list(map(int, np.array(image).flatten()))


def calculate_distance(image, centroid):
    def cosine_dist(first, second):
        sim = (first @ second) / (
                np.linalg.norm(first) * np.linalg.norm(second)
        )
        return 1 - sim

    def euklid_dist(first, second):
        return np.linalg.norm(first - second)

    first = np.array(image)
    second = np.array(centroid)
    if first.shape != second.shape:
        return None
    return cosine_dist(first, second)


def find_neares_centroid(image_path, centroids_array):
    centroid_index = None
    min_distance = None
    for number, centroid in enumerate(centroids_array):
        distance = calculate_distance(read_image(image_path), centroid)
        if distance is None:
            continue
        if min_distance is None or distance < min_distance:
            min_distance = distance
            centroid_index = number
    return centroid_index


def calculate_new_centroid(images_paths):
    if len(images_paths):
        images_array = np.array([
            np.array(read_image(image_path))
            for image_path in images_paths
        ])
        return list(images_array.mean(axis=0))
    return None


def calculate_new_centroid_array(centroids_info):
    new_centroid_array = [None for _ in range(K)]
    for centroid, centroid_index in centroids_info:
        new_centroid_array[centroid_index] = centroid
    return new_centroid_array


class MRImagesKMeans(MRJob):
    # image.jpg, [centroids] -> K, image.jpg
    def mapper_kmeans(self, image_path, centroids_array):
        centroid_index = find_neares_centroid(image_path, centroids_array)
        yield centroid_index, image_path

    # K, [images] -> image.jpg, (centroid, K)
    def reducer_kmeans(self, centroid_index, images_paths):
        images_paths = list(images_paths)
        centroid = calculate_new_centroid(images_paths)
        for image_path in images_paths:
            yield image_path, (centroid, centroid_index)

    # image.jpg, (centroid, K) -> None, (image.jpg, (centroid, K))
    def mapper_centroids_union(self, image_path, centroid_info):
        yield None, (image_path, centroid_info)

    # None, (image.jpg, (centroid, K)) -> image.jpg, [centroids]
    def reducer_centroids_union(self, _, image_path_and_centroid_info):
        images, centroids_info = zip(*image_path_and_centroid_info)
        new_centroid_array = calculate_new_centroid_array(centroids_info)
        for image_path in images:
            yield image_path, new_centroid_array

    # None, image.jpg -> None, (image.jpg, (centroid, K))
    def mapper_start(self, _, image_path):
        yield None, (
            image_path, (read_image(image_path), random.randint(0, K-1)),
        )

    # K, [images] -> image.jpg, K
    def reducer_stop(self, centroid_index, images_paths):
        for image_path in images_paths:
            yield image_path, centroid_index

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_start,
                reducer=self.reducer_centroids_union,
            ),
            MRStep(
                mapper=self.mapper_kmeans,
                reducer=self.reducer_kmeans,
            ),
            MRStep(
                mapper=self.mapper_centroids_union,
                reducer=self.reducer_centroids_union,
            ),
            MRStep(
                mapper=self.mapper_kmeans,
                reducer=self.reducer_stop,
            ),
        ]


def stop_iteration(old_output, output):
    old_dict = {}
    for key, value in old_output:
        old_dict[key] = value[1]
    for key, value in output:
        if old_dict[key] != value[1]:
            return False
    return True


def print_output(output):
    result = defaultdict(list)
    for image_path, centroid in output:
        result[centroid].append(image_path)
    pprint(result)


if __name__ == '__main__':
    images_paths = map(
        lambda x: x.split('/')[-1],
        glob(f'{IMAGES_DIR}*.jpg'),
    )

    MR = MRImagesKMeans()

    output = MR.map_pairs(pairs=[
        (None, image_path) for image_path in images_paths
    ], step_num=0)
    output = MR.reduce_pairs(pairs=[pair for pair in output], step_num=0)
    output = list(output)
    old_output = output

    break_loop = False
    for i in range(1, MAX_ITERATIONS_NUMBER + 1):
        print(f'Iteration {i}...')

        output = MR.map_pairs(pairs=[pair for pair in output], step_num=1)
        output = list(output)
        print(output)

        output = MR.reduce_pairs(pairs=[pair for pair in output], step_num=1)

        output = list(output)
        if stop_iteration(old_output, output):
            break_loop = True
        old_output = output

        output = MR.map_pairs(pairs=[pair for pair in output], step_num=2)
        output = MR.reduce_pairs(pairs=[pair for pair in output], step_num=2)

        if break_loop and i >= MIN_ITERATIONS_NUMBER:
            break

    output = MR.map_pairs(pairs=[pair for pair in output], step_num=3)
    output = MR.reduce_pairs(pairs=[pair for pair in output], step_num=3)

    print_output(output)
