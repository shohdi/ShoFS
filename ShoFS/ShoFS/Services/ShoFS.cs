using System;
using System.Collections.Generic;
using System.IO;
using Cassandra;
using DiskAccessLibrary.FileSystems.Abstractions;
using SMBLibrary.Services;

namespace ShoFSNameSpace.Services
{

    public class MyDBModel
    {
        public MyDBModel()
        {
            this.ServerIP = new List<string>();
        }

        
        public List<string> ServerIP { get; set; }

        
        public int? Port { get; set; } = 9042;

        
        public string UserName { get; set; }

        
        public string Password { get; set; }



       
        public string KeySpace { get; set; }


    }


    public class ShoFS : IFileSystem
    {
        private string osSeperator = "";
        private MyDBModel myDBData = new MyDBModel();
        private int blockSize = 4096;
        public ShoFS(string FileSystemName,List<string> serverList ,string username,string password,string keyspace,int? port , int? block_size )
        {
            osSeperator = System.IO.Path.DirectorySeparatorChar.ToString();
            this.Name = FileSystemName;
            this.Size = long.MaxValue - 1;
            this.FreeSpace = long.MaxValue - 1;
            this.SupportsNamedStreams = true;

            this.myDBData = new MyDBModel();
            this.myDBData.ServerIP = serverList;
            if (port != null)
            {
                this.myDBData.Port = port;
            }

            this.myDBData.UserName = username;
            this.myDBData.Password = password;

            this.myDBData.KeySpace = keyspace;

            //test db
            var cluster = Cluster.Builder()
                         .AddContactPoints(this.myDBData.ServerIP).WithPort(this.myDBData.Port.Value).WithCredentials(this.myDBData.UserName, this.myDBData.Password)
                         .Build();

            var session = cluster.Connect(this.myDBData.KeySpace);
            createSettings(session);


        }

        private void createSettings(ISession session)
        {
            session.Execute("CREATE TABLE IF NOT EXISTS settings (block_size int primary key) ;");

            var rows = session.Execute("select * from settings;");
            bool foundSettings = false;
            foreach (var row in rows)
            {
                this.blockSize = row.GetValue<int>("block_size");
                foundSettings = true;
            }

            if (!foundSettings)
            {
                var settingsQuery = session.Prepare("insert into settings (block_size) VALUES (?) ;");
                session.Execute(settingsQuery.Bind(this.blockSize));
                foundSettings = true;
            }
        }

        public string Name { get; set; }

        public long Size { get; set; }

        public long FreeSpace { get; set; }

        public bool SupportsNamedStreams { get; set; }

        public FileSystemEntry CreateDirectory(string path)
        {
            throw new NotImplementedException();
        }

        public FileSystemEntry CreateFile(string path)
        {
            throw new NotImplementedException();
        }

        public void Delete(string path)
        {
            throw new NotImplementedException();
        }

        public FileSystemEntry GetEntry(string path)
        {
            throw new NotImplementedException();
        }

        public List<KeyValuePair<string, ulong>> ListDataStreams(string path)
        {
            throw new NotImplementedException();
        }

        public List<FileSystemEntry> ListEntriesInDirectory(string path)
        {
            throw new NotImplementedException();
        }

        public void Move(string source, string destination)
        {
            throw new NotImplementedException();
        }

        public Stream OpenFile(string path, FileMode mode, FileAccess access, FileShare share, FileOptions options)
        {
            throw new NotImplementedException();
        }

        public void SetAttributes(string path, bool? isHidden, bool? isReadonly, bool? isArchived)
        {
            throw new NotImplementedException();
        }

        public void SetDates(string path, DateTime? creationDT, DateTime? lastWriteDT, DateTime? lastAccessDT)
        {
            throw new NotImplementedException();
        }
    }
}
