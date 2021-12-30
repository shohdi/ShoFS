using System;
using System.Collections.Generic;
using System.IO;
using Cassandra;
using DiskAccessLibrary.FileSystems.Abstractions;
using Newtonsoft.Json;
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
        Cluster cluster = null;
        ISession session = null;
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
             cluster = Cluster.Builder()
                         .AddContactPoints(this.myDBData.ServerIP).WithPort(this.myDBData.Port.Value).WithCredentials(this.myDBData.UserName, this.myDBData.Password)
                         .Build();

            session = cluster.Connect(this.myDBData.KeySpace);
            createSettings(session);

            session.Execute("create table if not exists meta (path text , name text ,  entry text , primary key (path , name));");


            session.Execute("create table if not exists chunk (path text , name text , pos bigint , chunk_data blob   , primary key (path , name,pos));");
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
            string[] paths;
            string parentPath;
            List<string> allExceptMe ;
            path = getPathData(path, out paths, out parentPath,out allExceptMe);
            if (allExceptMe.Count > 0)
            {
                CreateDirectory(parentPath);
            }

            session = cluster.Connect(this.myDBData.KeySpace);

            var qr = session.Prepare("select * from meta where path = ? and name = ? ;");

            var rows = session.Execute(qr.Bind(parentPath == "" ? "root" : parentPath, paths[paths.Length - 1]));
            string ent = "";
            foreach (var row in rows)
            {
                ent = row.GetValue<string>("entry");
                return JsonConvert.DeserializeObject<FileSystemEntry>(ent);
            }

            FileSystemEntry entry = new FileSystemEntry(path, paths[paths.Length - 1], true, 0, DateTime.Now, DateTime.Now, DateTime.Now, false, false, false);
            ent = JsonConvert.SerializeObject(entry);

            var qrCreate = session.Prepare("insert into meta (path,name,entry) values (?,?,?) ;");
            session.Execute(qrCreate.Bind(parentPath == "" ? "root" : parentPath, paths[paths.Length - 1], ent));


            return entry;
        }

        private string getPathData(string path, out string[] paths, out string parentPath , out List<string> allExceptMe)
        {
            paths = checkPath(ref path);
            allExceptMe = new List<string>();
            for (int i = 0; i < paths.Length - 1; i++)
            {
                allExceptMe.Add(paths[i]);
            }

            parentPath = "";
            if (allExceptMe.Count > 0)
            {
                //found parent
                parentPath = this.osSeperator + String.Join(this.osSeperator[0], allExceptMe);

            }

           

            return path;
        }

        private string[] checkPath(ref string path)
        {
            if (string.IsNullOrWhiteSpace(path))
            {
                throw new ArgumentException("wrong path!");
            }
            path = path.Trim();

            if (path.EndsWith(this.osSeperator))
            {
                path = path.Substring(0, path.Length - 1);
            }

            string[] paths = path.Split(new string[] { this.osSeperator }, StringSplitOptions.RemoveEmptyEntries);
            if (paths.Length < 1)
            {
                throw new ArgumentException("wrong path!");
            }

            return paths;
        }

        public FileSystemEntry CreateFile(string path)
        {
            string[] paths;
            string parentPath;
            List<string> allExceptMe;
            path = getPathData(path, out paths, out parentPath, out allExceptMe);

            session = cluster.Connect(this.myDBData.KeySpace);

            var qr = session.Prepare("select * from meta where path = ? and name = ? ;");

            var rows = session.Execute(qr.Bind(parentPath == "" ? "root" : parentPath, paths[paths.Length - 1]));
            string ent = "";
            foreach (var row in rows)
            {
                ent = row.GetValue<string>("entry");
                var entryCheck = JsonConvert.DeserializeObject<FileSystemEntry>(ent);
                if (entryCheck.IsDirectory)
                {
                    throw new ArgumentException("Directory with same name already exists!");
                }
                else
                {
                    return entryCheck;
                }
            }

            FileSystemEntry entry = new FileSystemEntry(path, paths[paths.Length - 1], false, 0, DateTime.Now, DateTime.Now, DateTime.Now, false, false, false);
            ent = JsonConvert.SerializeObject(entry);

            var qrCreate = session.Prepare("insert into meta (path,name,entry) values (?,?,?) ;");
            session.Execute(qrCreate.Bind(parentPath == "" ? "root" : parentPath, paths[paths.Length - 1], ent));


            return entry;


        }

        public void Delete(string path)
        {
            string[] paths;
            string parentPath;
            List<string> allExceptMe;
            path = getPathData(path, out paths, out parentPath, out allExceptMe);

            session = cluster.Connect(this.myDBData.KeySpace);

            var qr = session.Prepare("delete from meta where path = ? and name = ? ;");
            session.Execute(qr.Bind(parentPath == "" ? "root" : parentPath, paths[paths.Length - 1]));
            qr = session.Prepare("delete from chunk where path = ? and name = ? ;");
            session.Execute(qr.Bind(parentPath == "" ? "root" : parentPath, paths[paths.Length - 1]));



        }

        public FileSystemEntry GetEntry(string path)
        {
            string[] paths;
            string parentPath;
            List<string> allExceptMe;
            path = getPathData(path, out paths, out parentPath, out allExceptMe);

            session = cluster.Connect(this.myDBData.KeySpace);

            var qr = session.Prepare("select * from meta where path = ? and name = ? ;");

            var rows = session.Execute(qr.Bind(parentPath == "" ? "root" : parentPath, paths[paths.Length - 1]));
            string ent = "";
            foreach (var row in rows)
            {
                ent = row.GetValue<string>("entry");
                return JsonConvert.DeserializeObject<FileSystemEntry>(ent);
            }


            throw new FileNotFoundException();


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
