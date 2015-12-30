// -----------------------------------------------------------------------
//  <copyright file="Utils.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Runtime.CompilerServices;

namespace NewBlittable
{
    public static class Utils
    {
        //TODO: replace

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetNextPowerOfTwo(int number) => (int)Math.Pow(2, Math.Ceiling(Math.Log(number, 2)));
    }
}