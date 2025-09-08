// ZipfWrapper.hpp
#pragma once

extern "C" {
    #include "genzipf.h" // Make sure this includes the function declarations from the C code
}

class ZipfWrapper {
public:
    ZipfWrapper(double alpha, uint64_t n) {
        // Initialize the Zipf generator with given parameters
        genzipf_init(alpha, n);
    }

    uint64_t getZipf() {
        // Get a Zipfian-distributed value
        return genzipf();
    }
};

