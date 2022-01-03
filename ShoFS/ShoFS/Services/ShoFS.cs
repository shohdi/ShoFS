using System;
using System.Collections.Generic;
using System.IO;
using Cassandra;
using DiskAccessLibrary.FileSystems.Abstractions;
using Newtonsoft.Json;
using SMBLibrary.Services;

namespace ShoFSNameSpace.Services
{

    public class FSStream : Stream
    {
        MyDBModel dbModel = null;
        PathData path = null;
        FileMode mode ;
        FileAccess access ;
        FileShare share;
        FileOptions options;
        Cluster cluster;
        long blockSize;
        private long writePosition;
        private List<byte> writeBytes = new List<byte>();

        public FSStream(MyDBModel _dbModel,PathData _path,FileMode _mode,FileAccess _access,FileShare _share , FileOptions _options,Cluster _cluster,long _blockSize)
        {
            this.dbModel = _dbModel;
            this.path = _path;
            this.mode = _mode;
            this.access = _access;
            this.share = _share;
            this.options = _options;
            this.cluster = _cluster;
            this.blockSize = _blockSize;

            if (this.access == FileAccess.Read || this.access == FileAccess.ReadWrite)
            {
                this._canRead = true;
            }
            else
            {
                this._canRead = false;
            }

            if (this.access == FileAccess.Write || this.access == FileAccess.ReadWrite)
            {
                this._canWrite = true;
            }
            else
            {
                this._canWrite = false;
            }

            this._canSeek = true;

            this._length = (long)this.path.pathEntry.Size;

            if(mode == FileMode.Append)
            {
                this.Position = this._length;
            }
            else
            {
                this.Position = 0;
            }

            this.writePosition = this.Position;
        }

        private bool _canRead = true;
        public override bool CanRead { get { return _canRead; } }


        private bool _canSeek = true;
        public override bool CanSeek { get { return _canSeek; } }

        private bool _canWrite = false;
        public override bool CanWrite { get { return _canWrite; } }


        private long _length = 0;
        public override long Length {get { return _length; } }


        public override long Position { get; set; } = 0;

        public override void Write(byte[] buffer, int offset, int count)
        {
            int written = 0;
            while (written < count)
            {
                this.writeBytes.Add(buffer[offset + written]);
                written++;
                if ((Position + written) % this.blockSize == 0)
                {
                    this.Flush();
                }
            }
        }

        public override void Flush()
        {
            if (this.writeBytes == null || this.writeBytes.Count == 0)
            {
                this.writeBytes = new List<byte>();
                return;
            }
            var pos = (int)(this.Position / this.blockSize);
            byte[] foundArr = null;
            using (var session = cluster.Connect(this.dbModel.KeySpace))
            {
                var qr = session.Prepare("select * from chunk where id=? and pos=? ;");
                var row = this.firstOrDefault(session.Execute(qr.Bind(this.path.path_id,pos)));
                if(row != null)
                {
                    foundArr = row.GetValue<byte[]>("chunk_data");
                    qr = session.Prepare("delete  from chunk where id=? and pos=? ;");
                    session.Execute(qr.Bind(this.path.path_id,pos));


                }

                List<byte> lstChunk = new List<byte>();

                if (foundArr == null || foundArr.Length == 0)
                {
                    lstChunk = createChunk(pos);
                }
                else
                {
                    lstChunk.AddRange(foundArr);
                }


                var currentPos  = this.Position - (pos * this.blockSize);
                int place = 0;
                while (currentPos < this.blockSize )
                {
                    if(currentPos >= lstChunk.Count)
                    {
                        lstChunk.Add(this.writeBytes[place]);
                        

                    }
                    else
                    {
                        lstChunk[(int)currentPos] = this.writeBytes[place];
                    }

                    place++;
                    this.Position++;
                    currentPos = this.Position - (pos * this.blockSize);
                }

                qr = session.Prepare("insert into chunk (id,pos,chunk_data) values (?,?,?) ;");
                session.Execute(qr.Bind(this.path.path_id,pos,lstChunk.ToArray()));
                this.writeBytes = new List<byte>();



            }
        }

        private List<byte> createChunk(long pos)
        {
            List<byte> ret = new List<byte>();
            long count = this.Position - (pos * this.blockSize);
            byte[] bt = new byte[count];
            ret.AddRange(bt);
            return ret;
        }

        private Row firstOrDefault(RowSet rows)
        {
            Row ret = null;
            foreach (var row in rows)
            {
                ret = row;
                break;
            }

            return ret;
        }


        public override int Read(byte[] buffer, int offset, int count)
        {
            
            
            int readed = 0;

            using (var session = cluster.Connect(dbModel.KeySpace))
            {
                
                while (readed < count && Position < Length)
                {
                    var pos = (long)(this.Position / this.blockSize);
                    var qr = session.Prepare("select * from chunk where id=? and pos=? ;");
                    var rows = session.Execute(qr.Bind(this.path.path_id,pos));
                    Row row = null;
                    foreach(var rowin in rows)
                    {
                        row = rowin;
                        break;
                    }

                    byte[] btData = new byte[0];
                    if(row != null)
                    {
                        btData = row.GetValue<byte[]>("chunk_data");

                    }
                    
                    while(btData != null && btData.Length > 0 && this.Position < ((pos * blockSize) + btData.Length) && readed < count && Position < Length)
                    {
                        var ind = this.Position - ((pos * blockSize));
                        buffer[offset + readed] = btData[ind];
                        readed++;
                        this.Position++;
                    }

                }

                return readed;
            }
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            if(origin == SeekOrigin.Begin)
            {
                this.Position = offset;
            }
            else if (origin == SeekOrigin.Current)
            {
                this.Position = this.Position + offset;
            }
            else if(origin == SeekOrigin.End)
            {
                this.Position = this.Length - offset;
            }

            return this.Position;
        }

        public override void SetLength(long value)
        {
            this._length = value;
            this.path.pathEntry.Size = (ulong)value;
            using (var session = cluster.Connect(dbModel.KeySpace))
            {
                var qr = session.Prepare("update meta set entry = ? where parent_id = ? and id = ? ;");
                session.Execute(qr.Bind(JsonConvert.SerializeObject(this.path.pathEntry), this.path.parent_id,this.path.path_id));

            }
        }

        
    }

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


                session.Execute("create table if not exists chunk ( id text , pos bigint , chunk_data blob   , primary key ( id,pos));");
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
                qr = session.Prepare("delete from chunk where id = ? ;");
                session.Execute(qr.Bind( pathData.path_id));
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

            var sourceData = this.getPathData(source);
            if(sourceData.pathEntry == null)
            {
                throw new FileNotFoundException();
            }

            var destData = this.getPathData(destination);
            if(destData.pathEntry != null)
            {
                this.Delete(destData.path);
                destData = this.getPathData(destination);
            }

            if(destData.parent_path != "root")
            {
                this.CreateDirectory(destData.parent_path);
                destData = this.getPathData(destination);
            }

            System.Console.WriteLine("move " + sourceData.path + " to " + destData.path );

            destData.path_id = sourceData.path_id;
            destData.pathEntry = sourceData.pathEntry;
            destData.pathEntry.FullName = destData.path;

            using (var session = cluster.Connect(this.myDBData.KeySpace))
            {
                var qr = session.Prepare("insert into path_id (path ,id) values (?,?) ;");
                session.Execute(qr.Bind(destData.path, destData.path_id));
                qr = session.Prepare("delete from path_id where path = ? ;");
                session.Execute(qr.Bind(sourceData.path));
                qr = session.Prepare("update id_path set path = ? where id = ? ;");
                session.Execute(qr.Bind(destData.path,destData.path_id));

                qr = session.Prepare("insert into meta (parent_id ,id,entry) values (?,?,?) ;");
                session.Execute(qr.Bind(destData.parent_id, destData.path_id,JsonConvert.SerializeObject(destData.pathEntry)));

                qr = session.Prepare("delete from meta where parent_id = ? and id = ? ;");
                session.Execute(qr.Bind(sourceData.parent_id,sourceData.path_id));

                


            }

            updateChilds(destData);

        }

        private void updateChilds(PathData destData)
        {
            using (var session = cluster.Connect(this.myDBData.KeySpace))
            {
                var qr = session.Prepare("select * from meta where parent_id = ? ;");
                var rows = session.Execute(qr.Bind(destData.path_id));
                foreach (var row in rows)
                {
                    var ent = row.GetValue<string>("entry");
                    var entry = JsonConvert.DeserializeObject<FileSystemEntry>(ent);
                    var rowid = row.GetValue<string>("id");

                    var rowPath = destData.path == "root" ? "" : destData.path + this.osSeperator + entry.Name;

                    qr = session.Prepare("select * from id_path where id = ? ;");
                    var oldRows = session.Execute(qr.Bind(rowid));
                    string oldPath = null;
                    foreach (var oldRow in oldRows)
                    {
                        oldPath = oldRow.GetValue<string>("path");
                    }

                    qr = session.Prepare("insert into path_id (path,id) values (?,?) ;");
                    session.Execute(qr.Bind(rowPath, rowid));

                    if (oldPath != null)
                    {
                        qr = session.Prepare("delete from path_id where path = ? ;");
                        session.Execute(qr.Bind(oldPath));
                    }

                    qr = session.Prepare("update id_path set path = ? where id = ? ;");
                    session.Execute(qr.Bind(rowPath, rowid));
                    if (entry.IsDirectory)
                    {
                        PathData rowData = new PathData();
                        rowData.parent_id = destData.path_id;
                        rowData.parent_path = destData.path;
                        rowData.path = rowPath;
                        rowData.path_id = rowid;
                        rowData.pathEntry = entry;
                        rowData.name = entry.Name;
                        this.updateChilds(rowData);
                    }
                }
            }
        }

        public Stream OpenFile(string path, FileMode mode, FileAccess access, FileShare share, FileOptions options)
        {
            if (mode == FileMode.Create || mode == FileMode.CreateNew || mode == FileMode.OpenOrCreate)
            {
                var entry =  CreateFile(path);
                
            }

            var pathData = getPathData(path);
            return new FSStream(this.myDBData, pathData, mode, access, share, options,this.cluster,this.blockSize);

        }

        public void SetAttributes(string path, bool? isHidden, bool? isReadonly, bool? isArchived)
        {

            var myData = this.getPathData(path);

            if (myData.pathEntry == null)
            {
                throw new FileNotFoundException();
            }

            System.Console.WriteLine("SetAttributes " + myData.path);

            if(isHidden != null)
                myData.pathEntry.IsHidden = isHidden.GetValueOrDefault();
            if (isReadonly != null)
                myData.pathEntry.IsReadonly = isReadonly.GetValueOrDefault();
            if (isArchived != null)
                myData.pathEntry.IsArchived = isArchived.GetValueOrDefault();
            updateEntry(myData);

        }

        private void updateEntry(PathData myData)
        {
            using (var session = cluster.Connect(myDBData.KeySpace))
            {
                var ent = JsonConvert.SerializeObject(myData.pathEntry);
                var qr = session.Prepare("update meta set entry = ? where parent_id = ? and id = ? ;");
                session.Execute(qr.Bind(ent, myData.parent_id, myData.path_id));
            }
        }

        public void SetDates(string path, DateTime? creationDT, DateTime? lastWriteDT, DateTime? lastAccessDT)
        {
            var myData = this.getPathData(path);

            if (myData.pathEntry == null)
            {
                throw new FileNotFoundException();
            }

            System.Console.WriteLine("SetDates " + myData.path);

            if (creationDT != null)
                myData.pathEntry.CreationTime = creationDT.GetValueOrDefault();
            if (lastWriteDT != null)
                myData.pathEntry.LastWriteTime = lastWriteDT.GetValueOrDefault();
            if (lastAccessDT != null)
                myData.pathEntry.LastAccessTime = lastAccessDT.GetValueOrDefault();

            updateEntry(myData);
        }

        
    }
}
