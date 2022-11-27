#define USE_BOOST
#include "Df/DataFrame.hpp"
#include <chrono>
#include <future>
static const df::data_map<int64_t, std::string> BASE_DATA = {
    {"id", {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
    {"age", {10, 20, 30, 40, 50, 60, 70, 80, 90, 100}}};
static const df::DataFrame<int64_t, std::string> BASE_DF(BASE_DATA);
class Timer
{
public:
    Timer() : start(std::chrono::high_resolution_clock::now()) {}
    Timer(const std::string_view &name) : start(std::chrono::high_resolution_clock::now()), function_name(name) {}
    Timer(const std::string_view &name, std::string &result_str) : start(std::chrono::high_resolution_clock::now()),
                                                                   function_name(name)
    {
        ptr_result = &result_str;
    }
    ~Timer()
    {
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        std::string result = std::string(function_name.value_or("Time")) + ": " + std::to_string(duration.count()) + " microseconds\n";
        if (ptr_result)
        {
            *ptr_result += result;
        }
        else
        {

            std::cout << result;
        }
    }

private:
    std::chrono::time_point<std::chrono::high_resolution_clock> start;
    std::optional<std::string_view> function_name;
    std::string *ptr_result = nullptr;
};
void SelectTest(bool &result,std::string& result_str)
{
    try
    {
        Timer t("SelectTest",result_str);
        df::DataFrame df = BASE_DF;
        bool teste1 = (df.getCols() == 2);
        bool teste2 = (df.getRows() == 10);
        df::DataFrame<int64_t, std::string> df2 = df.select(df::column_set{"id"});
        bool teste3 = (df2.getCols() == 1);
        bool teste4 = (df2.getRows() == 10);
        result = teste1 && teste2 && teste3 && teste4;
    }
    catch (std::exception &e)
    {
        std::cout << e.what() << std::endl;
        result = false;
    }
}
void ConcatTest(bool &result,std::string& result_str)
{
    try
    {
        Timer t("ConcatTest",result_str);
        df::DataFrame df = BASE_DF;
        bool teste1 = (df.getRows() == 10);
        df::data_map<int64_t, std::string> data2 = {
            {"id", {11, 12, 13, 14, 15, 16, 17, 18, 19, 20}},
            {"age", {110, 120, 130, 140, 150, 160, 170, 180, 190, 200}}};
        df::DataFrame<int64_t, std::string> df2(data2);
        bool teste2 = (df2.getRows() == 10);
        df.concat(df2);
        bool teste3 = (df.getRows() == 20);
        result = teste1 && teste2 && teste3;
    }
    catch (std::exception &e)
    {
        std::cout << e.what() << std::endl;
        result = false;
    }
}
void GetRowsAndColsTest(bool &result,std::string& result_str)
{
    try
    {
        Timer t("GetRowsAndColsTest",result_str);
        df::DataFrame df = BASE_DF;
        bool teste1 = (df.getRows() == 10);
        bool teste2 = (df.getCols() == 2);
        result = teste1 && teste2;
    }
    catch (std::exception &e)
    {
        std::cout << e.what() << std::endl;
        result = false;
    }
}
void LeftJoinTest(bool &result,std::string& result_str)
{
    try
    {
        Timer t("LeftJoinTest",result_str);
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
        result = teste1 && teste2 && teste3;
    }
    catch (std::exception &e)
    {
        std::cout << e.what() << std::endl;
        result = false;
    }
}
void JoinMultiple(bool &result,std::string& result_str)
{
    try
    {
        Timer t("JoinMultiple",result_str);
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
        result = teste1 && teste2 && teste3;
    }
    catch (std::exception &e)
    {
        std::cout << e.what() << std::endl;
        result = false;
    }
}
void JoinNA(bool &result,std::string& result_str)
{
    try
    {
        Timer t("JoinNA",result_str);
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
        result = teste1 && teste2 && teste3;
    }
    catch (std::exception &e)
    {
        std::cout << e.what() << std::endl;
        result = false;
    }
}
void FindTest(bool &result,std::string& result_str)
{
    try
    {
        Timer t("FindTest",result_str);
        df::DataFrame df = BASE_DF;
        bool teste1 = (df.getCols() == 2);
        bool teste2 = (df.getRows() == 10);
        df::DataFrame<int64_t, std::string> df2 = df.find("id", 1);
        bool teste3 = (df2.getCols() == 2);
        bool teste4 = (df2.getRows() == 1);
        result = teste1 && teste2 && teste3 && teste4;
    }
    catch (std::exception &e)
    {
        std::cout << e.what() << std::endl;
        result = false;
    }
}
void WriteCsvTest(bool &result,std::string& result_str)
{
    try
    {
        Timer t("WriteCsvTest",result_str);
        df::DataFrame df = BASE_DF;
        {
            Timer t1("W,result_strritting");
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
        result = teste1 && teste2 && teste3;
    }
    catch (std::exception &e)
    {
        std::cout << e.what() << std::endl;
        result = false;
    }
}
void LoadTest(bool &result,std::string& result_str)
{
    try
    {
        Timer t("LoadTest",result_str);
        df::data_map<int64_t, std::string> data = {
            {"id", {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
            {"age", {10, 20, 30, 40, 50, 60, 70, 80, 90, 100}}};
        df::DataFrame<int64_t, std::string> df;
        df.load(data);
        bool teste1 = df.getRows() == 10;
        bool teste2 = df.getCols() == 2;
        result = teste1 && teste2;
    }
    catch (std::exception &e)
    {
        std::cout << e.what() << std::endl;
        result = false;
    }
}
#ifdef USE_BOOST
void WriteXlsxTest(bool &result,std::string& result_str)
{
    try
    {
        Timer t("WriteXlsxTest",result_str);
        df::data_map<std::string, double> data = {
            {"id", {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
            {"age", {10, 20, 30, 40, 50, 60, 70, 80, 90, 100}},
            {"name", {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}},
            {"type", {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}},
            {"value", {1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9, 10.10}}};
        df::DataFrame df(data);
        df.write_xlsx(std::string("teste.xlsx"));
        fs::remove("teste.xlsx");
        result = true;
    }
    catch (std::exception &e)
    {
        std::cout << e.what() << std::endl;
        result = false;
    }
}
#else
void WriteXlsxTest(bool &result,std::string& result_str)
{
    Timer t("WriteXlsxTest",result_str);
    result = true;
}
#endif
void SortTest(bool &result,std::string& result_str)
{
    try
    {
        Timer t("SortTest",result_str);
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
        result = teste1 && teste2 && teste3;
    }
    catch (std::exception &e)
    {
        std::cout << e.what() << std::endl;
        result = false;
    }
}
void ConcatMultiple(bool &result,std::string& result_str)
{
    try
    {
        Timer t("ConcatMultiple",result_str);
        df::data_map<int64_t, std::string> data = {
            {"id", {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
            {"age", {10, 20, 30, 40, 50, 60, 70, 80, 90, 100}}};
        df::DataFrame<int64_t, std::string> df(data);
        df::DataFrame<int64_t, std::string> df2(data);
        for (int i = 0; i < 100; i++)
        {
            df.concat(df2);
        }
        bool teste1 = df.getRows() == 1010;
        bool teste2 = df.getCols() == 2;
        result = teste1 && teste2;
    }
    catch (std::exception &e)
    {
        std::cout << e.what() << std::endl;
        result = false;
    }
}
int main()
{
    std::string result_str;
    Timer t("Main");
    std::string ss;
    // Create a vector with all the functions
    df::vector<std::function<void(bool &result,std::string& result_str)>> functions = {
        SelectTest,
        ConcatTest,
        LoadTest,
        // WriteXlsxTest,
        SortTest,
        ConcatMultiple,
        FindTest,
        SortTest,
        LeftJoinTest,
        JoinNA,
        JoinMultiple,
        WriteCsvTest};
    df::vector<bool> results(functions.size());
    std::vector<std::future<void>> futures(functions.size());
    // Create a thread for each function
    for (int i = 0; i < functions.size(); i++)
    {
        futures[i] = std::async(std::launch::async, functions[i], std::ref(results[i]),std::ref(result_str));
    }
    // Wait for all threads to finish
    for (int i = 0; i < functions.size(); i++)
    {
        
    }
    std::cout << result_str << "\n";
    // Print the results
    for (int i = 0; i < functions.size(); i++)
    {
        ss += "Test " + std::to_string(i) + " " + (results[i] ? "OK\n" : "FAIL\n");
    }
    std::cout << ss << std::endl;
    return 0;
}