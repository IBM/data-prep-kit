from scipy.integrate import quad as integrate


def fuzzy_optimal_param(
        threshold: float,
        num_perm: int,
        false_positive_weight: float,
        false_negative_weight: float,
) -> tuple[int, int]:
    """
    Computes parameters for fuzzy dedup
    :param threshold: filtering threshold
    :param num_perm: number of permutations
    :param false_positive_weight: false positive weight
    :param false_negative_weight: false negative weight
    :return: number of buckets and bucket length
    """
    def _false_positive_probability(ths: float, b: int, r: int) -> float:
        """
        Compute false positive probability
        :param ths: filtering threshold
        :param b: permutation
        :param r: rel permutation
        :return: probability
        """
        _probability = lambda s: 1 - (1 - s ** float(r)) ** float(b)
        a, err = integrate(_probability, 0.0, ths)
        return a

    def _false_negative_probability(ths: float, b: int, r: int) -> float:
        """
        Compute false negative probability
        :param ths: filtering threshold
        :param b: permutation
        :param r: rel permutation
        :return: probability
        """
        _probability = lambda s: 1 - (1 - (1 - s ** float(r)) ** float(b))
        a, err = integrate(_probability, ths, 1.0)
        return a

    min_error = float("inf")
    opt = (0, 0)
    for perm in range(1, num_perm + 1):
        max_r = int(num_perm / perm)
        for rel in range(1, max_r + 1):
            fp = _false_positive_probability(threshold, perm, rel)
            fn = _false_negative_probability(threshold, perm, rel)
            error = fp * false_positive_weight + fn * false_negative_weight
            if error < min_error:
                min_error = error
                opt = (perm, rel)
    return opt
