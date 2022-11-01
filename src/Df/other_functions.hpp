#include "class_header.hpp"
namespace df
{
    template <typename... T>
    vector<bool> isna(const series<T...> &series)
    {
        vector<bool> result;
        for (auto &item : series)
        {
            result.push_back(std::holds_alternative<na_value>(item));
        }
        return result;
    }
}