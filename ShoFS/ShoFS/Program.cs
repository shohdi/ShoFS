using System;
using System.Collections.Generic;
using ShoFSNameSpace.Services;
using SMBLibrary.Server;

namespace ShoFSNameSpace
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("start test db!");
            ShoFS shohdi = new ShoFS("Shohdi File System", new List<string> { "127.0.0.1" }, "sa", "P@ssw0rd", "shohdi_file_system", null, null);



        }
    }


}
