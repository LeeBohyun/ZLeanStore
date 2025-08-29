#include <cmath>
#include <iostream>
#include <random>
#include <stdexcept>
#include <vector>

// written by gabriel haas
// lia
// added mapping so that frequently accessed values

class RejectionInversionZipfSampler {
private:
  const long numberOfElements;
  const double exponent;
  double hIntegralX1;
  double hIntegralNumberOfElements;
  double s;

  std::uniform_real_distribution<double> dis;

public:
  RejectionInversionZipfSampler(long _numberOfElements, double _exponent)
      : numberOfElements(_numberOfElements), exponent(_exponent) {
    if (_numberOfElements <= 0) {
      throw std::invalid_argument(
          "number of elements is not strictly positive: " +
          std::to_string(_numberOfElements));
    }

    hIntegralX1 = hIntegral(1.5) - 1;
    hIntegralNumberOfElements = hIntegral(numberOfElements + 0.5);
    s = 2 - hIntegralInverse(hIntegral(2.5) - h(2));
    dis = std::uniform_real_distribution<double>(hIntegralX1,
                                                 hIntegralNumberOfElements);
  }

  long sample(std::mt19937_64 &rng) {
    while (true) {
      double u = dis(rng);
      double x = hIntegralInverse(u);
      long k = static_cast<long>(x + 0.5);

      if (k < 1) {
        k = 1;
      } else if (k > numberOfElements) {
        k = numberOfElements;
      }

      if (k - x <= s || u >= hIntegral(k + 0.5) - h(k)) {
        return k;
        return permute(k); // Map k to a value with no memory cost
      }
    }
  }

private:
  double hIntegral(const double x) const {
    const double logX = std::log(x);
    return helper2((1 - exponent) * logX) * logX;
  }

  double h(const double x) const { return std::exp(-exponent * std::log(x)); }

  double hIntegralInverse(const double x) const {
    double t = x * (1 - exponent);
    if (t < -1) {
      t = -1;
    }
    return std::exp(helper1(t) * x);
  }

  static double helper1(const double x) {
    if (std::abs(x) > 1e-8) {
      return std::log1p(x) / x;
    } else {
      return 1 - x * (0.5 - x * (0.33333333333333333 - 0.25 * x));
    }
  }

  static double helper2(const double x) {
    if (std::abs(x) > 1e-8) {
      return std::expm1(x) / x;
    } else {
      return 1 + x * 0.5 * (1 + x * 0.33333333333333333 * (1 + 0.25 * x));
    }
  }

  // Pseudo-random bijection mapping [1, N] -> [1, N]
  long permute(long k) const {
    // Mix the input k with a hash-like function
    uint64_t x = static_cast<uint64_t>(k);
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return 1 + (x % numberOfElements); // Keep result in [1, N]
  }
};