#ifndef DATAGENERATION_CPP
#define DATAGENERATION_CPP

#include "utilities.cpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <random>
#include <set>
#include <vector>

template <typename T>
std::vector<T> generateLinearDistribution(int numPoints, T minValue, T maxValue) {
  std::vector<T> points;
  auto step = (maxValue - minValue) / (numPoints - 1);
  for(auto i = 0; i < numPoints; i++) {
    points.push_back(minValue + i * step);
  }
  if(points.back() != maxValue) {
    points.push_back(maxValue);
  }
  return points;
}

template <typename T>
std::vector<T> generateLogDistribution(int numPoints, double minValue, double maxValue) {
  std::vector<T> points;

  for(auto i = 0; i < numPoints; ++i) {
    auto t = i / static_cast<double>(numPoints - 1); // Normalized parameter between 0 and 1
    auto value =
        std::pow(10.0, t * (std::log10(maxValue) - std::log10(minValue)) + std::log10(minValue));
    points.push_back(static_cast<T>(value));
  }

  auto unique_end = std::unique(points.begin(), points.end());
  points.erase(unique_end, points.end());

  if(points[points.size() - 1] != static_cast<int>(maxValue)) {
    points[points.size() - 1] = static_cast<int>(maxValue);
  }

  return points;
}

template <typename T>
std::vector<T> generateEvenIntLogDistribution(int numPoints, T minValue, T maxValue) {
  static_assert(std::is_integral<T>::value, "Must be an integer type");
  auto points = generateLogDistribution<T>(numPoints, minValue, maxValue);

  T tmp;
  for(auto& point : points) {
    tmp = point;
    if(tmp % 2 == 1 && tmp != maxValue) {
      tmp++;
    }
    point = tmp;
  }

  auto unique_end = std::unique(points.begin(), points.end());
  points.erase(unique_end, points.end());

  return points;
}

template <typename T>
std::vector<T> generateUniformDistribution(size_t n, T lowerBound, T upperBound,
                                           unsigned int seed = 1) {
  static_assert(std::is_integral<T>::value, "Must be an integer type");

  std::mt19937 gen(seed);

  std::uniform_int_distribution<T> distribution(lowerBound, upperBound);

  std::vector<T> data;
  data.reserve(n);

  for(size_t i = 0; i < n; ++i) {
    data.push_back(distribution(gen));
  }
  return data;
}

template <typename T>
std::vector<T> generatePartiallySortedOneToOneHundred(int n, int numRepeats,
                                                      float percentageRandom) {
  static_assert(std::is_integral<T>::value, "Must be an integer type");
  assert(percentageRandom >= 0.0 && percentageRandom <= 100.0);

  if(static_cast<int>(percentageRandom) == 100) {
    return generateUniformDistribution<T>(n, 1, 100);
  }

  std::vector<T> data;
  data.reserve(n);

  int sectionSize = 100 * numRepeats;
  int sections = n / (sectionSize);
  int elementsToShufflePerSection =
      static_cast<int>(0.5 * (percentageRandom / 100.0) * static_cast<float>(sectionSize));

#if False
  std::cout << sectionSize << " section size" << std::endl;
  std::cout << sections << " sections" << std::endl;
  std::cout << elementsToShufflePerSection << " pairs to shuffle per section" << std::endl;
#endif

  unsigned int seed = 1;
  std::mt19937 gen(seed);

  bool increasing = true;
  T lowerBound = 1;
  T upperBound = 100;

  T value;
  auto index = 0;
  for(auto i = 0; i < sections; ++i) {
    value = increasing ? lowerBound : upperBound;
    for(auto j = 0; j < 100; ++j) {
      for(auto k = 0; k < numRepeats; ++k) {
        data.push_back(value);
        ++index;
      }
      value = increasing ? value + 1 : value - 1;
    }
    increasing = !increasing;

    std::set<int> selectedIndexes = {};
    int index1, index2;
    for(auto swapCount = 0; swapCount < elementsToShufflePerSection; ++swapCount) {
      std::uniform_int_distribution<int> dis(1, sectionSize);

      index1 = dis(gen);
      while(selectedIndexes.count(index1) > 0) {
        index1 = dis(gen);
      }

      index2 = dis(gen);
      while(selectedIndexes.count(index2) > 0) {
        index2 = dis(gen);
      }

      selectedIndexes.insert(index1);
      selectedIndexes.insert(index2);
      std::swap(data[index - index1], data[index - index2]);

#if False
      std::cout << "Swapped indexes: " << index1 << ", " << index2 << std::endl;
#endif
    }

#if False
    for(int x = 0; x < 100; ++x) {
      for(int y = 0; y < numRepeats; ++y) {
        std::cout << data[index - sectionSize + (x * numRepeats) + y] << std::endl;
      }
    }
#endif
  }
  return data;
}

std::vector<uint32_t> generateClusteringOrder(int n, int spreadInCluster, int seed_ = 1) {
  uint32_t numBuckets = 1 + n - spreadInCluster;
  std::vector<std::vector<uint32_t>> buckets(numBuckets, std::vector<uint32_t>());

  uint32_t seed = seed_;
  std::mt19937 gen(seed);

  for(int i = 0; i < n; i++) {
    std::uniform_int_distribution<uint32_t> distribution(std::max(0, 1 + i - spreadInCluster),
                                                         std::min(i, n - spreadInCluster));
    buckets[distribution(gen)].push_back(i);
  }

  std::vector<uint32_t> result;
  for(uint32_t i = 0; i < numBuckets; i++) {
    std::shuffle(buckets[i].begin(), buckets[i].end(), gen);
    result.insert(result.end(), buckets[i].begin(), buckets[i].end());
  }

  return result;
}

template <typename T>
boss::Span<T> applyClusteringOrderToSpan(const boss::Span<T>& orderedSpan,
                                         boss::Span<T>&& clusteredSpan,
                                         const std::vector<uint32_t>& positions, int n) {
  for(auto i = 0; i < n; i++) {
    clusteredSpan[i] = orderedSpan[positions[i]];
  }
  return clusteredSpan;
}

template <typename T>
void runClusteringOnData(std::vector<T>& data, int spreadInCluster, int seed = 1) {
  auto positions = generateClusteringOrder(data.size(), spreadInCluster, seed);
  std::vector<T> dataCopy = data;
  for(size_t i = 0; i < data.size(); i++) {
    data[i] = dataCopy[positions[i]];
  }
}

template <typename T>
inline T scaleNumberLogarithmically(T number, int startingUpperBound, int targetUpperBound) {
  double scaledValue = log(number) / log(startingUpperBound);
  double scaledNumber = pow(targetUpperBound, scaledValue);
  return std::round(scaledNumber);
}

template <typename T>
std::vector<T> generateUniformDistributionWithSetCardinality(int n, int upperBound, int cardinality,
                                                             int seed = 1) {
  assert(n >= cardinality);

  if(cardinality == 1) {
    return std::vector<T>(n, upperBound);
  }

  int baselineDuplicates = n / cardinality;
  int remainingValues = n % cardinality;
  std::vector<T> data;
  data.reserve(n);

  for(int section = 0; section < cardinality; section++) {
    for(int elemInSection = 0; elemInSection < (baselineDuplicates + (section < remainingValues));
        elemInSection++) {
      data.push_back(section + 1);
    }
  }

  std::default_random_engine rng(seed);
  std::shuffle(data.begin(), data.end(), rng);

  if(upperBound != cardinality) {
    for(int i = 0; i < n; i++) {
      data[i] = scaleNumberLogarithmically(data[i], cardinality, upperBound);
    }
  }

  return data;
}

template <typename T>
std::vector<T>
generateUniformDistributionWithSetCardinalityClustered(int n, int upperBound, int cardinality,
                                                       int spreadInCluster, int seed = 1) {
  assert(n >= cardinality);

  if(cardinality == 1) {
    return std::vector<T>(n, upperBound);
  }

  int baselineDuplicates = n / cardinality;
  int remainingValues = n % cardinality;
  std::vector<T> data;
  data.reserve(n);

  for(int section = 0; section < cardinality; section++) {
    for(int elemInSection = 0; elemInSection < (baselineDuplicates + (section < remainingValues));
        elemInSection++) {
      data.push_back(section + 1);
    }
  }

  runClusteringOnData(data, spreadInCluster, seed);

  if(upperBound != cardinality) {
    for(int i = 0; i < n; i++) {
      data[i] = scaleNumberLogarithmically(data[i], cardinality, upperBound);
    }
  }

  return data;
}

template <typename T>
std::vector<T> generateStepChangeLowerBoundUniformDistVaryingLengthOfSection(
    size_t n, T lowerBound1, T lowerBound2, T upperBound, size_t sectionLength,
    unsigned int seed = 1) {
  static_assert(std::is_integral<T>::value, "Must be an integer type");

  std::vector<T> data;
  data.reserve(n);

  std::mt19937 gen(seed);

  bool inSection1 = true;
  T lowerBound;

  size_t index = 0;
  size_t tuplesToAdd;
  while(index < n) {
    if(inSection1) {
      lowerBound = lowerBound1;
    } else {
      lowerBound = lowerBound2;
    }

    std::uniform_int_distribution<T> distribution(lowerBound, upperBound);
    tuplesToAdd = std::min(sectionLength, n - index);
    for(size_t j = 0; j < tuplesToAdd; ++j) {
      data.push_back(distribution(gen));
      index++;
    }

    inSection1 = !inSection1;
  }
  return data;
}

template <typename T>
std::vector<T> generateStepChangeLowerBoundUniformDistVaryingPercentageSectionOne(
    size_t n, T lowerBound1, T lowerBound2, T upperBound, size_t numberOfPairsOfSections,
    float fractionSection1, unsigned int seed = 1) {
  static_assert(std::is_integral<T>::value, "Must be an integer type");

  std::vector<T> data;
  data.reserve(n);

  size_t elementsSectionPair = n / numberOfPairsOfSections;

  auto elementsPerSection1 =
      static_cast<size_t>(static_cast<float>(elementsSectionPair) * fractionSection1);
  auto elementsPerSection2 = static_cast<size_t>(elementsSectionPair - elementsPerSection1);

  std::mt19937 gen(seed);

  bool inSection1 = true;
  T lowerBound;

  size_t index = 0;
  size_t tuplesToAdd;
  while(index < n) {
    if(inSection1) {
      lowerBound = lowerBound1;
    } else {
      lowerBound = lowerBound2;
    }

    std::uniform_int_distribution<T> distribution(lowerBound, upperBound);
    if(inSection1) {
      tuplesToAdd = std::min(elementsPerSection1, n - index);
    } else {
      tuplesToAdd = std::min(elementsPerSection2, n - index);
    }
    for(size_t j = 0; j < tuplesToAdd; ++j) {
      data.push_back(distribution(gen));
      index++;
    }

    inSection1 = !inSection1;
  }
  return data;
}

template <typename T>
std::vector<T> generateUniformDistributionWithApproximateCardinality(int n, T upperBound,
                                                                     int cardinality,
                                                                     int seed = 1) {
  std::mt19937 gen(seed);

  std::vector<T> data;
  data.reserve(n);

  if(cardinality == 1) {
    for(auto i = 0; i < n; ++i) {
      data.push_back(upperBound);
    }
  } else {
    std::uniform_int_distribution<T> distribution(1, cardinality);
    for(auto i = 0; i < n; ++i) {
      data.push_back(scaleNumberLogarithmically(distribution(gen), cardinality, upperBound));
    }
  }

  return data;
}

template <typename T>
std::vector<T> generateUniformDistWithTwoCardinalitySectionsOfDifferentLengths(
    size_t n, T upperBound, int cardinalitySectionOne, int cardinalitySectionTwo,
    float fractionSectionOne, unsigned int seed = 1) {
  static_assert(std::is_integral<T>::value, "Must be an integer type");
  assert(cardinalitySectionOne <= upperBound && cardinalitySectionTwo <= upperBound);

  auto sizeSectionOne = static_cast<size_t>(static_cast<float>(n) * fractionSectionOne);
  auto sizeSectionTwo = n - sizeSectionOne;

  auto sectionOneData = generateUniformDistributionWithApproximateCardinality<T>(
      sizeSectionOne, upperBound, cardinalitySectionOne, seed);
  auto sectionTwoData = generateUniformDistributionWithApproximateCardinality<T>(
      sizeSectionTwo, upperBound, cardinalitySectionTwo, seed);

  std::vector<T> data;
  data.insert(data.end(), sectionOneData.begin(), sectionOneData.end());
  data.insert(data.end(), sectionTwoData.begin(), sectionTwoData.end());

  return data;
}

#endif // DATAGENERATION_CPP