// Copyright (c) 2026 TSKV Authors. All rights reserved.
// Status class for error handling

#pragma once

#include <memory>
#include <string>
#include <cstring>

namespace tskv {

class Status {
public:
    // Create a success status
    Status() noexcept : code_(kOk), state_(nullptr) {}

    ~Status() = default;

    // Copy
    Status(const Status& s);
    Status& operator=(const Status& s);

    // Move
    Status(Status&& s) noexcept;
    Status& operator=(Status&& s) noexcept;

    // Status codes
    enum Code : unsigned char {
        kOk = 0,
        kNotFound = 1,
        kCorruption = 2,
        kNotSupported = 3,
        kInvalidArgument = 4,
        kIOError = 5,
    };

    // Create status with specific code
    static Status OK() { return Status(); }

    static Status NotFound(const std::string& msg = "") {
        return Status(kNotFound, msg);
    }

    static Status Corruption(const std::string& msg = "") {
        return Status(kCorruption, msg);
    }

    static Status NotSupported(const std::string& msg = "") {
        return Status(kNotSupported, msg);
    }

    static Status InvalidArgument(const std::string& msg = "") {
        return Status(kInvalidArgument, msg);
    }

    static Status IOError(const std::string& msg = "") {
        return Status(kIOError, msg);
    }

    // Returns true if the status is success
    bool ok() const { return code_ == kOk; }

    bool IsNotFound() const { return code_ == kNotFound; }
    bool IsCorruption() const { return code_ == kCorruption; }
    bool IsNotSupported() const { return code_ == kNotSupported; }
    bool IsInvalidArgument() const { return code_ == kInvalidArgument; }
    bool IsIOError() const { return code_ == kIOError; }

    Code code() const { return code_; }

    // Return a string representation
    std::string ToString() const;

private:
    Status(Code code, const std::string& msg);

    Code code_;
    std::shared_ptr<std::string> state_;  // nullptr means no message
};

inline Status::Status(Code code, const std::string& msg)
    : code_(code), state_(msg.empty() ? nullptr : std::make_shared<std::string>(msg)) {}

inline Status::Status(const Status& s) : code_(s.code_), state_(s.state_) {}

inline Status& Status::operator=(const Status& s) {
    if (this != &s) {
        code_ = s.code_;
        state_ = s.state_;
    }
    return *this;
}

inline Status::Status(Status&& s) noexcept
    : code_(s.code_), state_(std::move(s.state_)) {
    s.code_ = kOk;
}

inline Status& Status::operator=(Status&& s) noexcept {
    if (this != &s) {
        code_ = s.code_;
        state_ = std::move(s.state_);
        s.code_ = kOk;
    }
    return *this;
}

inline std::string Status::ToString() const {
    const char* type;
    switch (code_) {
        case kOk:
            return "OK";
        case kNotFound:
            type = "NotFound: ";
            break;
        case kCorruption:
            type = "Corruption: ";
            break;
        case kNotSupported:
            type = "NotSupported: ";
            break;
        case kInvalidArgument:
            type = "InvalidArgument: ";
            break;
        case kIOError:
            type = "IOError: ";
            break;
        default:
            type = "Unknown: ";
            break;
    }
    std::string result(type);
    if (state_) {
        result.append(*state_);
    }
    return result;
}

}  // namespace tskv
