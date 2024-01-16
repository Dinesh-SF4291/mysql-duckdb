using System;
using System.Diagnostics;
using static System.Net.Mime.MediaTypeNames;
class Program
{
    private static string errorLogs;
    //static string error = "";
    public static void Main(String[] args)
    {
        //Console.WriteLine("Do you want to choose mysql as destination:(yes/no)");
        //string answer = Console.ReadLine();
        string answer = "yes";
        if (answer == "yes")
        {
            Console.WriteLine("mysql destination");
            string pythonscript = File.ReadAllText("D:\\Training\\test.py");
            string pypath = "C:\\Users\\DineshKumarSureshKum\\AppData\\Local\\Programs\\Python\\Python310";
            string pythonPath = Path.Combine(pypath, "python.exe");
            string error = "";
            bool flag = execute(string.Format("pip install pandas", Path.Combine(pypath, "Scripts")),error);
            //string temp=
            Console.WriteLine(errorLogs);
            errorLogs = "";
            bool isrun = execute(string.Format("{0} {1}", pythonPath, "D:\\Training\\test.py"), error);
            Console.WriteLine(errorLogs);
            Console.ReadKey();
        }
    }

    public static bool execute(string command,string error )
    {
        Process process = new Process();
        process.StartInfo.FileName = "cmd.exe";
        process.StartInfo.Arguments = $"/c \"{command}\"";
        process.StartInfo.UseShellExecute = false;
        process.StartInfo.RedirectStandardOutput = true;
        process.StartInfo.RedirectStandardError = true;
        //if (!string.IsNullOrEmpty(workingDirectory))
        //{
        //    process.StartInfo.WorkingDirectory = workingDirectory;
        //}
        process.OutputDataReceived += Process_OutputDataReceived;
        process.ErrorDataReceived += Process_ErrorDataReceived;
        process.Start();
        process.BeginOutputReadLine();
        process.BeginErrorReadLine();
        //   string output = process.StandardOutput.ReadToEnd();
        process.WaitForExit();
        int exitCode = process.ExitCode;
        error += errorLogs;
        return exitCode == 0;
        
    }
    private static void Process_ErrorDataReceived(object sender, DataReceivedEventArgs e)
    {
        if (!string.IsNullOrEmpty(e.Data))
        {
            // Handle standard output
            errorLogs += e.Data + "\r\n";
        }
    }

    private static void Process_OutputDataReceived(object sender, DataReceivedEventArgs e)
    {
        if (!string.IsNullOrEmpty(e.Data))
        {
            // Handle standard output
            errorLogs += e.Data + "\r\n";
        }
    }

}













//C: \Users\DineshKumarSureshKum\AppData\Local\Programs\Python\Python310\python.exe 
//C:\Users\DineshKumarSureshKum\OneDrive - Syncfusion\Desktop\Blazor\mysql\mysql\bin\Debug\net6.0\test_pipeline.py

//    C:\Users\DineshKumarSureshKum\AppData\Local\Programs\Python\Python310\python.exe 
//    D:\SnowFlake\app_data\elt\dltprojects\Snow\tap_postgres\tap_postgres_pipeline.py




