#include <memory>
#include <string>
#include <string_view>
#include <sstream>
#include <stdexcept>
#include <vector>
#include <cstring>

#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/x509.h>
#include <openssl/decoder.h>
#include <openssl/core_names.h>

#include "base64.h"

namespace tateyama::authentication {

template<typename T, typename D>
std::unique_ptr<T, D> make_handle(T* handle, D deleter)
{
    return std::unique_ptr<T, D>{handle, deleter};
}

class rsa_encrypter {
public:
    explicit rsa_encrypter(std::string_view public_key_text) : public_key_(public_key_text) {
        evp_public_key_ = make_handle(get_public_key(), EVP_PKEY_free);
        if (!evp_public_key_) {
            throw std::runtime_error("Get public key failed.");
        }
    }

    void encrypt(std::string_view in, std::string &out) {
        size_t buf_len = 0;

        auto ctx = make_handle(EVP_PKEY_CTX_new_from_pkey(nullptr, evp_public_key_.get(), nullptr), EVP_PKEY_CTX_free);
        if (!ctx) {
            throw std::runtime_error("fail to EVP_PKEY_CTX_new_from_pkey");
        }

        if (EVP_PKEY_encrypt_init(ctx.get()) <= 0) {
            throw std::runtime_error("EVP_PKEY_encrypt_init() failed.");
        }

        if (EVP_PKEY_CTX_set_rsa_padding(ctx.get(), RSA_PKCS1_OAEP_PADDING) <= 0) {
            throw std::runtime_error("EVP_PKEY_CTX_set_rsa_padding() failed.");
        }

        if (EVP_PKEY_CTX_set_rsa_oaep_md(ctx.get(), EVP_sha1()) <= 0) {
            throw std::runtime_error("EVP_PKEY_CTX_set_rsa_oaep_md() failed.");
        }

        if (EVP_PKEY_CTX_set_rsa_mgf1_md(ctx.get(), EVP_sha1()) <= 0) {
            throw std::runtime_error("EVP_PKEY_CTX_set_rsa_mgf1_md() failed.");
        }

        if (EVP_PKEY_encrypt(ctx.get(), nullptr, &buf_len, reinterpret_cast<const unsigned char*>(in.data()), in.length()) <= 0) {  // NOLINT
            throw std::runtime_error("fail to EVP_PKEY_encrypt first");
        }
        out.resize(buf_len);
        if (EVP_PKEY_encrypt(ctx.get(), reinterpret_cast<unsigned char*>(out.data()), &buf_len, reinterpret_cast<const unsigned char*>(in.data()), in.length()) <= 0) {  // NOLINT
            throw std::runtime_error("fail to EVP_PKEY_encrypt final");
        }
    }

private:
    std::string public_key_{};
    std::unique_ptr<EVP_PKEY, void (*)(EVP_PKEY*)> evp_public_key_{nullptr, nullptr};

    EVP_PKEY *get_public_key() {
        EVP_PKEY *pkey = nullptr;
        int selection{EVP_PKEY_PUBLIC_KEY};
        const unsigned char *data{};
        size_t data_len{};

        data = reinterpret_cast<const unsigned char*>(public_key_.data());  // NOLINT
        data_len = public_key_.length();
        auto dctx = make_handle(OSSL_DECODER_CTX_new_for_pkey(&pkey, "PEM", nullptr, "RSA",
                                                              selection, nullptr, nullptr), OSSL_DECODER_CTX_free);
        if (OSSL_DECODER_from_data(dctx.get(), &data, &data_len)  <= 0) {
            throw std::runtime_error("fail to handle public key");
        }
        return pkey;
    }
};

}  // tateyama::authentication
