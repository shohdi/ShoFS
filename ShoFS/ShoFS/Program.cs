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


            shohdi.CreateDirectory("/testdir/test/t1t1/");
            shohdi.CreateFile("/testdir/test/t1t1/shohdi.txt");
            shohdi.Delete("/testdir/test/t1t1");
        }
    }


}
