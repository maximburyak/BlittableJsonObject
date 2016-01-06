using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ConsoleApplication4;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace NewBlittable.Tests.Benchmark
{
    public class BigJsonsBenchmark
    {
        public static void PerformanceAnalysis()
        {
            using (var byteInAllHeapsConter = new PerformanceCounter(
                ".NET CLR Memory", "# Bytes in all Heaps", Process.GetCurrentProcess().ProcessName))
            using (var processProcessorTimeCounter = new PerformanceCounter(
            "Process", "% Processor Time", Process.GetCurrentProcess().ProcessName))
            using (var processPrivateBytes = new PerformanceCounter(
            "Process", "Private Bytes", Process.GetCurrentProcess().ProcessName))
            {
                var files = Directory.GetFiles(@"C:\Users\bumax_000\Downloads\JsonExamples", "*.json");

                foreach (var jsonFile in files)
                {
                    Console.WriteLine(jsonFile);
                    var jsonFileText = File.ReadAllText(jsonFile);

                    Stopwatch sp;
                    long simpleReadTime;
                    //var results = JsonProcessorRunner(()=>)
                    var before = SimpleJsonIteration(jsonFileText, out sp, out simpleReadTime);
                    Console.WriteLine(
                        $"Simple JSON Read Time:{simpleReadTime:#,#} mem: {GC.GetTotalMemory(false) - before:#,#} bytes");
                    before = GC.GetTotalMemory(true);
                    sp.Restart();

                    JObject.Load(new JsonTextReader(new StringReader(jsonFileText)));
                    var jobjectReadTime = sp.ElapsedMilliseconds;

                    Console.WriteLine(
                        $"Json Object Read Time:{jobjectReadTime} mem: {GC.GetTotalMemory(false) - before:#,#} bytes");

                    before = GC.GetTotalMemory(true);

                    sp.Restart();

                    using (var unmanagedPool = new UnmanagedBuffersPool(string.Empty, 1024 * 1024 * 1024))
                    using (var blittableContext = new BlittableContext(unmanagedPool))
                    using (
                        var employee = new BlittableJsonWriter(new JsonTextReader(new StringReader(jsonFileText)),
                            blittableContext,
                            "doc1"))
                    {
                        employee.Write();
                        var blittableObjectReadTime = sp.ElapsedMilliseconds;

                        Console.WriteLine($"Json Object Read Time:{blittableObjectReadTime} " +
                                          $"mem: {GC.GetTotalMemory(false) - before:#,#} bytes unmanaged " +
                                          $"{unmanagedPool.CurrentSize:#,#} bytes, size {employee.SizeInBytes:#,#}");
                    }
                }
            }
        }

        public class OperationResults
        {
            public Dictionary<string, List<CounterSample>> CountersValues;
        }

        //public OperationResults JsonProcessorRunner(Action processor, List<PerformanceCounter> counters)
        //{
        //    var results = new OperationResults
        //    {
        //        CountersValues = new Dictionary<string, List<CounterSample>>()
        //    };
        //    int signaled = 0;
        //    foreach (var counter in counters)
        //    {
        //        results.CountersValues.Add($"{counter.CategoryName}\\{counter.CounterName}",new List<CounterSample>());
        //    }

        //    var jsonProcessorTask = Task.Run(processor).ContinueWith(x=>Interlocked.Exchange(ref signaled, 1));
        //    var countersCollectorTask = Task.Run(() =>
        //    {
        //        while (Interlocked.CompareExchange(ref signaled,1,1)== 0)
        //        {
        //            foreach (var counter in counters)
        //            {
        //                results.CountersValues[$"{counter.CategoryName}\\{counter.CounterName}"].Add(counter.NextSample());
        //            }
        //            Thread.Sleep(30);
        //        }
        //    });
        //    Task.WaitAll(new[] {jsonProcessorTask, countersCollectorTask});
        //    return null;
        //}
        
        private static long SimpleJsonIteration(string jsonFileText, out Stopwatch sp, out long simpleReadTime)
        {
            var jsonStringReader = new JsonTextReader(new StringReader(jsonFileText));
            var before = GC.GetTotalMemory(true);
            sp = Stopwatch.StartNew();
            while (jsonStringReader.Read())
            {
            }
            simpleReadTime = sp.ElapsedMilliseconds;
            return before;
        }
    }
}
