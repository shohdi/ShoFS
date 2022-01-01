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

            using (var session = cluster.Connect(this.myDBData.KeySpace))
            {
                createSettings(session);

                session.Execute("create table if not exists path_id (path text primary key , id text );");

                session.Execute("create table if not exists id_path (id text primary key , path text );");

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
            ret.path = path;

            ret.parent_path = parentPath == "" ? "root" : parentPath;
            if(ret.parent_path == "root")
            {
                ret.parent_id = "root";
                if(!ret.path.StartsWith(this.osSeperator))
                {
                    ret.path = this.osSeperator + ret.path;
                }
            }
            

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
                        ret.pathEntry.FullName = ret.path;
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
            System.Console.WriteLine("create directory " + myPath.path);

           
            if (!string.IsNullOrWhiteSpace(myPath.parent_path) && myPath.parent_path != "root")
            {
                CreateDirectory(myPath.parent_path);
            }

            myPath = this.getPathData(path);

            if (myPath.pathEntry != null)
            {
                return myPath.pathEntry;
            }

            using (var session = cluster.Connect(this.myDBData.KeySpace))
            {



                FileSystemEntry entry = new FileSystemEntry(myPath.path, myPath.name, true, 0, DateTime.Now, DateTime.Now, DateTime.Now, false, false, false);
                string ent = JsonConvert.SerializeObject(entry);

                PreparedStatement qrCreate = null;
                qrCreate = create_path_id(myPath, session);

                qrCreate = session.Prepare("insert into meta (parent_id,id,entry) values (?,?,?) ;");
                session.Execute(qrCreate.Bind(myPath.parent_id, myPath.path_id, ent));


                return entry;
            }
        }

        private static PreparedStatement create_path_id(PathData myPath, ISession session)
        {
            PreparedStatement qrCreate = session.Prepare("insert into path_id (path,id) values (?,?);");
            myPath.path_id = System.Guid.NewGuid().ToString();
            session.Execute(qrCreate.Bind(myPath.path, myPath.path_id));

            qrCreate = session.Prepare("insert into id_path (id,path) values (?,?);");

            session.Execute(qrCreate.Bind(myPath.path_id, myPath.path));
            return qrCreate;
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
            var pathData = getPathData(path);
            System.Console.WriteLine("create file " + pathData.path);
            if (pathData.parent_path != "root")
            {
                CreateDirectory(pathData.parent_path);
            }

            if (pathData.pathEntry != null)
            {
                if( pathData.pathEntry.IsDirectory)
                {
                    throw new ArgumentException("Directory with same name already exists!");
                }
                else
                {
                    return pathData.pathEntry;
                }
            }

            FileSystemEntry entry = new FileSystemEntry(pathData.path, pathData.name, false, 0, DateTime.Now, DateTime.Now, DateTime.Now, false, false, false);
            var ent = JsonConvert.SerializeObject(entry);
            using (var session = cluster.Connect(this.myDBData.KeySpace))
            {
                
                var qrCreate = create_path_id(pathData, session);

                qrCreate = session.Prepare("insert into meta (parent_id,id,entry) values (?,?,?) ;");
                session.Execute(qrCreate.Bind(pathData.parent_id, pathData.path_id, ent));


                return entry;
            }

            

            

           


        }

        

        public void Delete(string path)
        {
            var pathData = getPathData(path);

            System.Console.WriteLine("delete path " + pathData.path);


            if (string.IsNullOrWhiteSpace(pathData.path_id))
            {
                throw new FileNotFoundException();
            }

            List<FileSystemEntry> childs = this.ListEntriesInDirectory(pathData.path);

            foreach (var child in childs)
            {
                Delete(child.FullName);
            }

            using (var session = cluster.Connect(this.myDBData.KeySpace))
            {
                deletePathId(pathData.path,pathData.path_id, session);
                var qr = session.Prepare("delete from meta where parent_id = ? and id = ? ;");
                session.Execute(qr.Bind(pathData.parent_id, pathData.path_id));
                qr = session.Prepare("delete from chunk where parent_id = ? and id = ? ;");
                session.Execute(qr.Bind(pathData.parent_id, pathData.path_id));
            }



        }

        private  void deletePathId(string path,string id, ISession session)
        {
            var qr = session.Prepare("delete from path_id where path = ? ;");
            session.Execute(qr.Bind(path));
            qr = session.Prepare("delete from id_path where id = ? ;");
            session.Execute(qr.Bind(id));
            
        }

        public FileSystemEntry GetEntry(string path)
        {
            var pathData = this.getPathData(path);

            if(pathData.pathEntry != null)
            {
                return pathData.pathEntry;
            }


            throw new FileNotFoundException();


        }

        public List<KeyValuePair<string, ulong>> ListDataStreams(string path)
        {
            
            List<KeyValuePair<string, ulong>> result = new List<KeyValuePair<string, ulong>>();
            var pathData = this.getPathData(path);
            System.Console.WriteLine("List Data Streams " + pathData.path);
            FileSystemEntry entry = pathData.pathEntry;
            if (entry != null)
            {
                if (!entry.IsDirectory)
                {
                    result.Add(new KeyValuePair<string, ulong>("::$DATA", entry.Size));
                }
            }
            return result;
        }

        public List<FileSystemEntry> ListEntriesInDirectory(string path)
        {
            path = path.Trim();
            path = path.Replace("\\", this.osSeperator);
            string parent_id = "";
            PathData pathData = null;
            if (path == "/" || path == "")
            {
                parent_id = "root";
            }
            else
            {
                pathData = this.getPathData(path);
                parent_id = pathData.path_id;
            }

            System.Console.WriteLine("list entries " + (parent_id=="root"?"/":pathData?.path));

            using (var session = cluster.Connect(myDBData.KeySpace))
            {
                var qr = session.Prepare("select * from meta where parent_id = ? ;");
                var rows = session.Execute(qr.Bind(parent_id));
                List<FileSystemEntry> entries = new List<FileSystemEntry>();
                foreach (var row in rows)
                {
                    var path_id = row.GetValue<string>("id");

                    qr = session.Prepare("select * from id_path where  id = ? ;");
                    var entryRows = session.Execute(qr.Bind(path_id));

                    foreach (var entryRow in entryRows)
                    {
                        var pathString = entryRow.GetValue<string>("path");

                        var entry = JsonConvert.DeserializeObject<FileSystemEntry>( row.GetValue<string>("entry"));
                        entry.FullName = pathString;

                        entries.Add(entry);

                    }

                }

                return entries;
            }
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
