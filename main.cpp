#define USE_BOOST
#include "Df/DataFrame.hpp"
#include <chrono>
static const df::data_map<int64_t, std::string> BASE_DATA = {
    {"id", {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
    {"age", {10, 20, 30, 40, 50, 60, 70, 80, 90, 100}}};
static const df::DataFrame<int64_t, std::string> BASE_DF(BASE_DATA);
class Timer
{
public:
    Timer() : start(std::chrono::high_resolution_clock::now()) {}
    Timer(std::string_view name) : start(std::chrono::high_resolution_clock::now()), function_name(name) {}
    ~Timer()
    {
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        if (function_name.has_value())
        {
            std::cout << function_name.value() << ": " << duration.count() << " microseconds\n";
        }
        else
        {
            std::cout << "Time: " << duration.count() << " microseconds\n";
        }
    }

private:
    std::chrono::time_point<std::chrono::high_resolution_clock> start;
    std::optional<std::string_view> function_name;
};
bool SelectTest()
{
    try
    {
        Timer t("SelectTest");
        df::DataFrame df = BASE_DF;
        bool teste1 = (df.getCols() == 2);
        bool teste2 = (df.getRows() == 10);
        df::DataFrame<int64_t, std::string> df2 = df.select(df::column_set{"id"});
        bool teste3 = (df2.getCols() == 1);
        bool teste4 = (df2.getRows() == 10);
        return teste1 && teste2 && teste3 && teste4;
    }
    catch (std::exception &e)
    {
        std::cout << e.what() << std::endl;
        return false;
    }
}
bool ConcatTest()
{
    try
    {
        Timer t("ConcatTest");
        df::DataFrame df = BASE_DF;
        bool teste1 = (df.getRows() == 10);
        df::data_map<int64_t, std::string> data2 = {
            {"id", {11, 12, 13, 14, 15, 16, 17, 18, 19, 20}},
            {"age", {110, 120, 130, 140, 150, 160, 170, 180, 190, 200}}};
        df::DataFrame<int64_t, std::string> df2(data2);
        bool teste2 = (df2.getRows() == 10);
        df.concat(df2);
        bool teste3 = (df.getRows() == 20);
        return teste1 && teste2 && teste3;
    }
    catch (std::exception &e)
    {
        std::cout << e.what() << std::endl;
        return false;
    }
}
bool GetRowsAndColsTest()
{
    try
    {
        Timer t("GetRowsAndColsTest");
        df::DataFrame df = BASE_DF;
        bool teste1 = (df.getRows() == 10);
        bool teste2 = (df.getCols() == 2);
        return teste1 && teste2;
    }
    catch (std::exception &e)
    {
        std::cout << e.what() << std::endl;
        return false;
    }
}
bool LeftJoinTest()
{
    try
    {
        Timer t("LeftJoinTest");
        df::DataFrame df = BASE_DF;
        df::data_map<int64_t, std::string> data2 = {
            {"id", {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
            {"name", {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}}};
        df::DataFrame<int64_t, std::string> df2(data2);
        df::DataFrame<int64_t, std::string> df3 = df.join(df2, df::column_set{"id"}, df::JoinTypes::LEFT);
        df::data_map<int64_t, std::string> data3 = {
            {"id", {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
            {"age", {10, 20, 30, 40, 50, 60, 70, 80, 90, 100}},
            {"name", {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}}};
        df::DataFrame<int64_t, std::string> df4(data3);
        bool teste1 = (df3.getRows() == 10);
        bool teste2 = (df3.getCols() == 3);
        bool teste3 = (df3 == df4);
        return teste1 && teste2 && teste3;
    }
    catch (std::exception &e)
    {
        std::cout << e.what() << std::endl;
        return false;
    }
}
bool JoinMultiple()
{
    try
    {
        Timer t("JoinMultiple");
        df::data_map<int64_t, std::string> data = {
            {"id1", {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
            {"id2", {11, 12, 13, 14, 15, 16, 17, 18, 19, 20}},
            {"age", {10, 20, 30, 40, 50, 60, 70, 80, 90, 100}}};
        df::DataFrame<int64_t, std::string> df(data);
        df::data_map<int64_t, std::string> data2 = {
            {"id1", {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
            {"id2", {11, 12, 13, 14, 15, 16, 17, 18, 19, 20}},
            {"name", {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}}};
        df::DataFrame<int64_t, std::string> df2(data2);
        df::DataFrame<int64_t, std::string> df3 = df.join(df2, df::column_set{"id1", "id2"}, df::JoinTypes::INNER);
        df::data_map<int64_t, std::string> data3 = {
            {"id1", {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
            {"id2", {11, 12, 13, 14, 15, 16, 17, 18, 19, 20}},
            {"age", {10, 20, 30, 40, 50, 60, 70, 80, 90, 100}},
            {"name", {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}}};
        df::DataFrame<int64_t, std::string> df4(data3);
        bool teste1 = (df3.getRows() == 10);
        bool teste2 = (df3.getCols() == 4);
        bool teste3 = (df3 == df4);
        return teste1 && teste2 && teste3;
    }
    catch (std::exception &e)
    {
        std::cout << e.what() << std::endl;
        return false;
    }
}
bool JoinNA()
{
    try
    {
        Timer t("JoinNA");
        df::DataFrame df = BASE_DF;
        df::data_map<int64_t, std::string> data2 = {
            {"id", {1, 2, 3, 4, 5, 6, 7, 8, 9}},
            {"name", {"a", "b", "c", "d", "e", "f", "g", "h", "i"}}};
        df::DataFrame<int64_t, std::string> df2(data2);
        df::DataFrame<int64_t, std::string> df3 = df.join(df2, df::column_set{"id"}, df::JoinTypes::LEFT);
        df::data_map<int64_t, std::string> data3 = {
            {"id", {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
            {"age", {10, 20, 30, 40, 50, 60, 70, 80, 90, 100}},
            {"name", {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}}};
        df::DataFrame<int64_t, std::string> df4(data3);
        bool teste1 = (df3.getRows() == 10);
        bool teste2 = (df3.getCols() == 3);
        bool teste3 = (df3 != df4);
        return teste1 && teste2 && teste3;
    }
    catch (std::exception &e)
    {
        std::cout << e.what() << std::endl;
        return false;
    }
}
bool FindTest()
{
    try
    {
        Timer t("FindTest");
        df::DataFrame df = BASE_DF;
        bool teste1 = (df.getCols() == 2);
        bool teste2 = (df.getRows() == 10);
        df::DataFrame<int64_t, std::string> df2 = df.find("id", 1);
        bool teste3 = (df2.getCols() == 2);
        bool teste4 = (df2.getRows() == 1);
        return teste1 && teste2 && teste3 && teste4;
    }
    catch (std::exception &e)
    {
        std::cout << e.what() << std::endl;
        return false;
    }
}
bool WriteCsvTest()
{
    try
    {
        Timer t("WriteCsvTest");
        df::DataFrame df = BASE_DF;
        {
            Timer t1("Writting");
            df.write_csv(std::string("teste.csv"), ';');
        }
        df::DataFrame<int64_t, std::string> df2;
        df2.load_csv(std::string("teste.csv"), ';');
        bool teste1 = df2.getRows() == 10;
        bool teste2 = df2.getCols() == 2;
        auto valor = df2.get(0, 0);
#ifdef USE_BOOST
        bool teste3 = boost::get<std::string>(valor) == "1";
        fs::remove("teste.csv");
#else
        bool teste3 = std::get<int64_t>(valor) == 1;
        std::remove("teste.csv");
#endif
        return teste1 && teste2 && teste3;
    }
    catch (std::exception &e)
    {
        std::cout << e.what() << std::endl;
        return false;
    }
}
bool LoadTest()
{
    try
    {
        Timer t("LoadTest");
        df::data_map<int64_t, std::string> data = {
            {"id", {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
            {"age", {10, 20, 30, 40, 50, 60, 70, 80, 90, 100}}};
        df::DataFrame<int64_t, std::string> df;
        df.load(data);
        bool teste1 = df.getRows() == 10;
        bool teste2 = df.getCols() == 2;
        return teste1 && teste2;
    }
    catch (std::exception &e)
    {
        std::cout << e.what() << std::endl;
        return false;
    }
}
#ifdef USE_BOOST
bool WriteXlsxTest()
{
    try
    {
        Timer t("WriteXlsxTest");
        df::DataFrame df = BASE_DF;
        df.write_xlsx(std::string("teste.xlsx"));
        fs::remove("teste.xlsx");
        return true;
    }
    catch (std::exception &e)
    {
        std::cout << e.what() << std::endl;
        return false;
    }
}
#else
bool WriteXlsxTest()
{
    Timer t("WriteXlsxTest");
    return true;
}
#endif
bool SortTest()
{
    try
    {
        Timer t("SortTest");
        df::data_map<int64_t, std::string> data = {
            {"id", {1, 4, 2, 5, 3, 6, 7, 8, 9, 10}},
            {"age", {10, 40, 20, 50, 30, 60, 70, 80, 90, 100}}};
        df::DataFrame<int64_t, std::string> df(data);
        df::DataFrame df2 = df.sort("id");
        bool teste1 = df2.getRows() == 10;
        bool teste2 = df2.getCols() == 2;
        bool teste3 = true;
        for (int i = 0; i < 10; i++)
        {
            auto valor = df2.get(i, 0);
            auto valor2 = df2.get(i, 1);
#ifdef USE_BOOST
            teste3 = teste3 && boost::get<int64_t>(valor) == i + 1 && boost::get<int64_t>(valor2) == (i + 1) * 10;
#else
            teste3 = teste3 && std::get<int64_t>(valor) == i + 1 && std::get<int64_t>(valor2) == (i + 1) * 10;
#endif
        }
        return teste1 && teste2 && teste3;
    }
    catch (std::exception &e)
    {
        std::cout << e.what() << std::endl;
        return false;
    }
}
int main()
{
    Timer t("Main");
    std::stringstream ss;
    if (SelectTest())
    {
        ss << "Test 1 passed\n";
    }
    else
    {
        ss << "Test 1 failed\n";
    }
    if (ConcatTest())
    {
        ss << "Test 2 passed\n";
    }
    else
    {
        ss << "Test 2 failed\n";
    }
    if (GetRowsAndColsTest())
    {
        ss << "Test 3 passed\n";
    }
    else
    {
        ss << "Test 3 failed\n";
    }
    if (LeftJoinTest())
    {
        ss << "Test 4 passed\n";
    }
    else
    {
        ss << "Test 4 failed\n";
    }
    if (JoinMultiple())
    {
        ss << "Test 5 passed\n";
    }
    else
    {
        ss << "Test 5 failed\n";
    }
    if (JoinNA())
    {
        ss << "Test 6 passed\n";
    }
    else
    {
        ss << "Test 6 failed\n";
    }
    if (FindTest())
    {
        ss << "Test 7 passed\n";
    }
    else
    {
        ss << "Test 7 failed\n";
    }
    if (WriteCsvTest())
    {
        ss << "Test 8 passed\n";
    }
    else
    {
        ss << "Test 8 failed\n";
    }
    if (LoadTest())
    {
        ss << "Test 9 passed\n";
    }
    else
    {
        ss << "Test 9 failed\n";
    }
    if (WriteXlsxTest())
    {
        ss << "Test 10 passed\n";
    }
    else
    {
        ss << "Test 10 failed\n";
    }
    if (SortTest())
    {
        ss << "Test 11 passed\n";
    }
    else
    {
        ss << "Test 11 failed\n";
    }
    std::cout << ss.str();
    return 0;
}