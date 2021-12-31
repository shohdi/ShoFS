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


    public class PathData
    {
        public string parent_id { get; set; }

        public string path_id { get; set; }

        public string path { get; set; }

        public string parent_path { get; set; }


        public string name { get; set; }

        public FileSystemEntry pathEntry { get; set; }

       



    }


    public class ShoFS : IFileSystem
    {
        private string osSeperator = "/";
        private MyDBModel myDBData = new MyDBModel();
        private int blockSize = 4096;
        Cluster cluster = null;
        ISession session = null;
        public ShoFS(string FileSystemName,List<string> serverList ,string username,string password,string keyspace,int? port , int? block_size )
        {
            
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

            using (session = cluster.Connect(this.myDBData.KeySpace))
            {
                createSettings(session);

                session.Execute("create table if not exists path_id (path text primary key , id text );");

                session.Execute("create table if not exists meta (parent_id text , id text ,  entry text , primary key (parent_id , id));");


                session.Execute("create table if not exists chunk (parent_id text , id text , pos bigint , chunk_data blob   , primary key (parent_id , id,pos));");
            }
        }

        private PathData getPathData(string path)
        {
            path = path.Trim();
            PathData ret = new PathData();

            string[] paths;
            string parentPath;
            List<string> allExceptMe;
            path = getPathData(path, out paths, out parentPath, out allExceptMe);

            ret.name = paths[paths.Length - 1];
            
            ret.parent_path = parentPath == "" ? "root" : parentPath;
            if(ret.parent_path == "root")
            {
                ret.parent_id = "root";
            }
            ret.path = path;

            using (var session = cluster.Connect(myDBData.KeySpace))
            {
                var qr = session.Prepare("select * from path_id where path = ?");

                var rows = session.Execute(qr.Bind(ret.path));

                foreach (var row in rows)
                {
                    ret.path_id = row.GetValue<string>("id");
                    break;
                }

                if(ret.parent_path != "root")
                {
                    qr = session.Prepare("select * from path_id where path = ?");

                    rows = session.Execute(qr.Bind(ret.parent_path));

                    foreach (var row in rows)
                    {
                        ret.parent_id = row.GetValue<string>("id");

                        break;
                    }
                }
                


                if(!string.IsNullOrWhiteSpace(ret.path_id))
                {
                    qr = session.Prepare("select * from meta where parent_id  = ? and id=?");

                    rows = session.Execute(qr.Bind(ret.parent_id,ret.path_id));

                    foreach (var row in rows)
                    {
                        ret.pathEntry = JsonConvert.DeserializeObject<FileSystemEntry>( row.GetValue<string>("entry"));

                        break;
                    }
                }

            }


            return ret;

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

            PathData myPath = this.getPathData(path);


           
            if (!string.IsNullOrWhiteSpace(myPath.parent_path) && myPath.parent_path != "root")
            {
                CreateDirectory(myPath.parent_path);
            }

            myPath = this.getPathData(path);

            if (myPath.pathEntry != null)
            {
                return myPath.pathEntry;
            }

            using (session = cluster.Connect(this.myDBData.KeySpace))
            {

                

                FileSystemEntry entry = new FileSystemEntry(myPath.path, myPath.name, true, 0, DateTime.Now, DateTime.Now, DateTime.Now, false, false, false);
                string ent = JsonConvert.SerializeObject(entry);

                var qrCreate = session.Prepare("insert into path_id (path,id) values (?,?);");
                myPath.path_id = System.Guid.NewGuid().ToString();
                session.Execute(qrCreate.Bind(myPath.path, myPath.path_id ));


                qrCreate = session.Prepare("insert into meta (parent_id,id,entry) values (?,?,?) ;");
                session.Execute(qrCreate.Bind(myPath.parent_id,myPath.path_id, ent));


                return entry;
            }
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
            path = path.Replace("\\", this.osSeperator);

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

        private bool haveChilds(string path)
        {
            string[] paths;
            string parentPath;
            List<string> allExceptMe;
            path = getPathData(path, out paths, out parentPath, out allExceptMe);
            session = cluster.Connect(this.myDBData.KeySpace);

            var qr = session.Prepare("delete from meta where path = ?  ;");
            var rows =session.Execute(qr.Bind(path));

            bool haveChilds = false;
            foreach (var row in rows)
            {
                haveChilds = true;
                break;
            }

            return haveChilds;



        }

        public void Delete(string path)
        {
            string[] paths;
            string parentPath;
            List<string> allExceptMe;
            path = getPathData(path, out paths, out parentPath, out allExceptMe);

            if (haveChilds(path))
            {
                throw new FileLoadException("Directory not empty");
            }

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
            FileSystemEntry entry = GetEntry(path);
            List<KeyValuePair<string, ulong>> result = new List<KeyValuePair<string, ulong>>();
            if (!entry.IsDirectory)
            {
                result.Add(new KeyValuePair<string, ulong>("::$DATA", entry.Size));
            }
            return result;
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
