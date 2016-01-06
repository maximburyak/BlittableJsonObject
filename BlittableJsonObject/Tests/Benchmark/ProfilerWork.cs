﻿// -----------------------------------------------------------------------
//  <copyright file="ProfilerWork.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using System.Linq;
using ConsoleApplication4;
using Newtonsoft.Json;

namespace NewBlittable.Tests.Benchmark
{
    public class ProfilerWork
    {
        public static void Run(int take)
        {
            string directory = @"C:\Users\bumax_000\Downloads\JsonExamples";
            var files = Directory.GetFiles(directory, "*.json");
            using (var unmanagedPool = new UnmanagedBuffersPool(string.Empty, 1024 * 1024 * 1024))
            using (var blittableContext = new BlittableContext(unmanagedPool))
            {
                foreach (var file in files.OrderBy(x=> new FileInfo(x).Length).Take(take))
                {
                    var v = File.ReadAllBytes(file);
                    using (var employee =
                                   new BlittableJsonWriter(new JsonTextReader(new StreamReader(new MemoryStream(v))),
                                       blittableContext,
                                       "doc1"))
                    {
                        employee.Write();
                    }
                }
            }

        }
    }
}