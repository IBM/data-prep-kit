import logging
import os
from typing import List, Set

import mmh3
import numpy as np


class Murmur_MH:
    def __init__(self, num_perm=64, seed=42, hashfunc=None):
        self.seed = seed
        self.num_perm = num_perm  # the number of buckets, i.e. the vector length after self.minhash() call
        self.permutations = self._init_permutations(seed, num_perm)

    def _init_permutations(self, seed, num_perm):
        # see https://en.wikipedia.org/wiki/Universal_hashing#Avoiding_modular_arithmetic
        max_int = np.uint64((1 << 64) - 1)
        # initialize pseudo random number generator with given seed value
        gen = np.random.RandomState(seed)
        # get self.num_perm pseudo random numbers between 2 and max_int (excl)
        permutations = np.array(
            [gen.randint(0, max_int, dtype=np.uint64) for _ in range(num_perm)],
            dtype=np.uint64,
        ).T
        # make all even pseudo random numbers odd by adding 1
        permutations[permutations % 2 == 0] += 1
        return permutations

    def minhash(self, shingles: List[str]):
        """return np.array of minhash"""
        # see https://en.wikipedia.org/wiki/Universal_hashing#Avoiding_modular_arithmetic
        hash_values = np.array([mmh3.hash(shingle, signed=False) for shingle in shingles], dtype=np.uint64)
        return (
            np.right_shift(
                (hash_values * np.tile(self.permutations, (len(hash_values), 1)).T).T,
                32,
            )
            .astype(np.uint32)
            .min(axis=0)
        )

    def minhash2(self, shingles: List[str], doc_len: int):
        """
        for each shingle (i.e. a group of k-words) it generates a digest value based on
        mmh3-hash function (32-bit)

        return tuple (A, B)
            A = an array of values = np.array of minhash
            B = document_length = number of characters"""
        # see https://en.wikipedia.org/wiki/Universal_hashing#Avoiding_modular_arithmetic
        hash_values = np.array([mmh3.hash(shingle, signed=False) for shingle in shingles], dtype=np.uint64)
        return (
            np.right_shift(
                (hash_values * np.tile(self.permutations, (len(hash_values), 1)).T).T,
                32,
            )
            .astype(np.uint32)
            .min(axis=0),
            doc_len,
        )

    def minhash2_nosalt(self, shingles: List[str], doc_len: int, doc_id: int):
        """
        for each shingle (i.e. a group of k-words) it generates a digest value based on
        mmh3-hash function (32-bit)

        return tuple (A, B)
            A = an array of values = np.array of minhash
            B = document_length = number of characters"""
        # see https://en.wikipedia.org/wiki/Universal_hashing#Avoiding_modular_arithmetic
        hash_values = np.array([mmh3.hash(shingle, signed=False) for shingle in shingles], dtype=np.uint64)
        return (
            np.right_shift(
                (hash_values * np.tile(self.permutations, (len(hash_values), 1)).T).T,
                32,
            )
            .astype(np.uint32)
            .min(axis=0)
            .astype(np.int32)
            .tolist(),
            doc_len,
            doc_id,
        )

    @staticmethod
    def jaccard(mh1: np.array, mh2: np.array) -> float:
        """
        The Jaccard similarity measures the similarity between two sets of data
        to see which members are shared and distinct.

        The Jaccard similarity is calculated by dividing the number of observations
        in both sets by the number of observations in either set.

        Developed by Paul Jaccard, the index ranges from 0 to 1.
        The closer to 1, the more similar the two sets of data.

        As a document is represented by a set. We use Jaccard distance to see how similar between two documents.
        """
        assert len(mh1) == len(mh2)
        return np.count_nonzero(mh1 == mh2) / len(mh1)
