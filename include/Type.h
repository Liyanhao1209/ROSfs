#ifndef TYPE_H
#define TYPE_H

#include <cstdint>
#include <cassert>
#include <vector>
#include <algorithm>
#include <utility>

namespace SpatialStorage {
    template <typename T>
    class KeyType {
    private:
        std::vector<T> data;
        uint64_t dimensions;

    public:
        KeyType() : dimensions(0) {}
        KeyType(const std::vector<T>& initData) : data(initData), dimensions(initData.size()) {
            assert(initData.size() % 2 == 0);
        }

        KeyType(const KeyType<T>& other) : data(other.data), dimensions(other.dimensions) {}
        KeyType(KeyType<T>&& other) noexcept 
            : data(std::move(other.data)), dimensions(other.dimensions) {
            other.dimensions = 0;
        }

        KeyType<T>& operator=(const KeyType<T>& other) {
            if (this != &other) {
                data = other.data;
                dimensions = other.dimensions;
            }
            return *this;
        }

        KeyType<T>& operator=(KeyType<T>&& other) noexcept {
            if (this != &other) {
                data = std::move(other.data);
                dimensions = other.dimensions;
                other.dimensions = 0;
            }
            return *this;
        }

        ~KeyType() = default;

        size_t size() const {
            return data.size();
        }

        const std::vector<T>& getData() const {
            return data;
        }

        void setData(const std::vector<T>& newData) {
            assert(newData.size() % 2 == 0);
            data = newData;
            dimensions = newData.size();
        }

        T enlargement(const KeyType<T>& other) const {
            assert(other.size() == this->size());
            auto size = this->size();
            T res = static_cast<T>(1);
            for (size_t i = 0, j = size / 2; i < size / 2 && j < size; i++, j++) {
                res *= std::max(this->data[j], other.data[j]) - std::min(this->data[i], other.data[i]);
            }
            return res;
        }

        T area() const {
            auto size = this->size();
            T res = static_cast<T>(1);
            for (size_t i = 0, j = size / 2; i < size / 2 && j < size; i++, j++) {
                res *= this->data[j] - this->data[i];
            }
            return res;
        }

        void mbr_enlarge(const KeyType<T>& other) {
            assert(other.size() == this->size());
            auto size = this->size();
            for (size_t i = 0; i < size / 2; i++) {
                this->data[i] = std::min(this->data[i], other.data[i]);
            }
            for (size_t i = size / 2; i < size; i++) {
                this->data[i] = std::max(this->data[i], other.data[i]);
            }
        }

        bool operator>=(const KeyType<T>& other) const {
            assert(other.size() == this->size());

            auto size = this->size();
            for (size_t i = 0; i < size / 2; i++) {
                if (this->data[i] > other.data[i]) return false;
            }

            for (size_t i = size / 2; i < size; i++) {
                if (this->data[i] < other.data[i]) return false;
            }

            return true;
        }

        bool operator<=(const KeyType<T>& other) const {
            assert(other.size() == this->size());

            auto size = this->size();
            for (size_t i = 0; i < size / 2; i++) {
                if (this->data[i] < other.data[i]) return false;
            }

            for (size_t i = size / 2; i < size; i++) {
                if (this->data[i] > other.data[i]) return false;
            }

            return true;
        }

        bool operator==(const KeyType<T>& other) const {
            assert(other.size() == this->size());

            auto size = this->size();
            for (size_t i = 0; i < size; i++) {
                if (this->data[i] != other.data[i]) return false;
            }

            return true;
        }

        bool operator!=(const KeyType<T>& other) const {
            return !(*this == other);
        }

        bool operator>(const KeyType<T>& other) const {
            assert(other.size() == this->size());

            auto size = this->size();
            for (size_t i = 0; i < size / 2; i++) {
                if (this->data[i] >= other.data[i]) return false;
            }

            for (size_t i = size / 2; i < size; i++) {
                if (this->data[i] <= other.data[i]) return false;
            }

            return true;
        }

        bool operator<(const KeyType<T>& other) const {
            assert(other.size() == this->size());

            auto size = this->size();
            for (size_t i = 0; i < size / 2; i++) {
                if (this->data[i] <= other.data[i]) return false;
            }

            for (size_t i = size / 2; i < size; i++) {
                if (this->data[i] >= other.data[i]) return false;
            }

            return true;
        }

        const T& operator[](size_t index) const {
            assert(index < dimensions);
            return data[index];
        }

        T& operator[](size_t index) {
            assert(index < dimensions);
            return data[index];
        }

        bool IsOverlap(const KeyType<T>& other) const {
            assert(other.size() == this->size());

            auto size = this->size();

            for (size_t i = 0, j = size / 2; i < size / 2 && j < size; i++, j++) {
                auto x1l = this->data[i];
                auto x1r = this->data[j];
                auto x2l = other.data[i];
                auto x2r = other.data[j];

                if (!(x1l <= x2r && x1r >= x2l)) return false;
            }

            return true;
        }
    };
}

#endif