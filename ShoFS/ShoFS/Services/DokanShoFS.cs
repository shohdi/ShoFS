using Cassandra;
using DokanNet;
using Newtonsoft.Json;
using ShoFSNameSpace.Services;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.AccessControl;
using System.Security.Principal;
using System.Text;
using System.Text.RegularExpressions;

namespace DokanShoFSNamespace.Services
{
	public class PathData
	{
		public string parent_id { get; set; }

		public string path_id { get; set; }

		public string path { get; set; }

		public string parent_path { get; set; }


		public string name { get; set; }

		public FileSystemEntry pathEntry { get; set; }





	}

    public class ShoFileInfo : IDokanFileInfo
    {
		[JsonIgnore]
		public object Context
		{
			get
			{
				return _context;
			}
			set
			{
				_context = (ulong)value;
			}
		}

		private ulong _context;

		public ulong LongContext { get {
				return _context;
			} set {
				_context = value;
			} }


		public bool DeleteOnClose { get; set; } = false;
		public bool IsDirectory { get; set; }


		
		public bool NoCacheSet { get; set; }
        public bool NoCache { get { return NoCacheSet; } }


		public bool PagingIoSet { get; set; }

		public bool PagingIo { get { return PagingIoSet; } }


		public int ProcessIdSet { get; set; }

		public int ProcessId { get { return ProcessIdSet; } }


		public bool SynchronousIoSet { get; set; }

		public bool SynchronousIo { get { return SynchronousIoSet; } }


		public bool WriteToEndOfFileSet { get; set; }

		public bool WriteToEndOfFile { get { return WriteToEndOfFileSet; } }

		public WindowsIdentity Requester { get; set; }

        public WindowsIdentity GetRequestor()
        {
			return Requester;
        }

        public bool TryResetTimeout(int milliseconds)
        {
            return true;
        }
    }


    public class FileSystemEntry
	{
		public string FullName { get; set; }

		public string FileName
		{
			get;
			set;
		}

		public System.IO.FileAttributes Attributes
		{
			get;
			set;
		}

		public DateTime? CreationTime
		{
			get;
			set;
		}

		public DateTime? LastAccessTime
		{
			get;
			set;
		}

		public DateTime? LastWriteTime
		{
			get;
			set;
		}

		public long Length
		{
			get;
			set;
		}

		public DokanNet.FileAccess access { get; set; }

		public System.IO.FileShare share { get; set; }


		public System.IO.FileOptions options { get; set; }

		public ShoFileInfo FileInfo { get; set; }


	}

	public class DokanShoFS : DokanNet.IDokanOperations
	{
		private string osSeperator = "/";
		private MyDBModel myDBData = new MyDBModel();
		private int blockSize = 4096;
		Cluster cluster = null;

		private string _name;
		private long _size;
		private long _freeSpace;


		public DokanShoFS(string FileSystemName, List<string> serverList, string username, string password, string keyspace, int? port, int? block_size)
		{

			this._name = FileSystemName;
			this._size = long.MaxValue - 1;
			this._freeSpace = long.MaxValue - 1;


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

				session.Execute("create table if not exists name_id (name text,id text,parent_id text ,primary key(name,id) );");

				session.Execute("create table if not exists id_name (id text , name text ,primary key(id,name) );");

				session.Execute("create table if not exists meta (parent_id text , id text ,  entry text , primary key (parent_id , id));");


				session.Execute("create table if not exists chunk ( id text , pos bigint , chunk_data blob   , primary key ( id,pos));");

				session.Execute("create table if not exists path_access ( id text,user text ,read boolean,write boolean   , primary key ( id,user));");
			}
		}

		private string getPathData(string path, out string[] paths, out string parentPath, out List<string> allExceptMe)
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
			if (ret.parent_path == "root")
			{
				ret.parent_id = "root";
				if (!ret.path.StartsWith(this.osSeperator))
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

				if (ret.parent_path != "root")
				{
					qr = session.Prepare("select * from path_id where path = ?");

					rows = session.Execute(qr.Bind(ret.parent_path));

					foreach (var row in rows)
					{
						ret.parent_id = row.GetValue<string>("id");

						break;
					}
				}



				if (!string.IsNullOrWhiteSpace(ret.path_id))
				{
					qr = session.Prepare("select * from meta where parent_id  = ? and id=?");

					rows = session.Execute(qr.Bind(ret.parent_id, ret.path_id));

					foreach (var row in rows)
					{
						ret.pathEntry = JsonConvert.DeserializeObject<FileSystemEntry>(row.GetValue<string>("entry"));
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


		public void addAccess(string path, string user, bool read, bool write)
		{
			PathData myPath = this.getPathData(path);
			System.Console.WriteLine("add access " + path);
			using (var session = cluster.Connect(this.myDBData.KeySpace))
			{
				var qr = session.Prepare("delete from path_access where id=? and user=?;");
				session.Execute(qr.Bind(myPath.path_id, user));
				qr = session.Prepare("insert into path_access(id,user,read,write) values (?,?,?,?);");
				session.Execute(qr.Bind(myPath.path_id, user, read, write));
			}
		}

		public void revokeAccess(string path, string user, bool read, bool write)
		{
			PathData myPath = this.getPathData(path);
			System.Console.WriteLine("add access " + path);
			using (var session = cluster.Connect(this.myDBData.KeySpace))
			{
				var qr = session.Prepare("delete from path_access where id=? and user=?;");
				session.Execute(qr.Bind(myPath.path_id, user));

			}
		}

		public List<FileAccessModel> getAccess(string path)
		{
			List<FileAccessModel> lstRet = new List<FileAccessModel>();
			PathData myPath = this.getPathData(path);
			System.Console.WriteLine("get access " + path);
			using (var session = cluster.Connect(this.myDBData.KeySpace))
			{
				var qr = session.Prepare("select * from path_access where id=? ;");
				var rows = session.Execute(qr.Bind(myPath.path_id));
				foreach (var row in rows)
				{
					FileAccessModel item = new FileAccessModel();
					item.path = myPath.path;
					item.path_id = myPath.path_id;
					item.read = row.GetValue<bool>("read");
					item.write = row.GetValue<bool>("write");
					item.user = row.GetValue<string>("user");
					lstRet.Add(item);

					if (myPath.parent_id != "root" && myPath.parent_id != null)
					{
						var lstParent = this.getAccess(myPath.parent_path);
						var currentUsers = lstRet.Select(s => s.user).ToList();
						var parentAdd = lstParent.Where(p => !currentUsers.Contains(p.user)).ToList();

						lstRet.AddRange(parentAdd);
					}
				}

				return lstRet;

			}
		}


		public void Cleanup(string fileName, DokanNet.IDokanFileInfo info)
		{
			System.Console.WriteLine("Cleanup file " + fileName);
		}

		public void CloseFile(string fileName, DokanNet.IDokanFileInfo info)
		{
			System.Console.WriteLine("Close File " + fileName);
		}

		public DokanNet.NtStatus CreateFile(string fileName, DokanNet.FileAccess access, FileShare share, FileMode mode, FileOptions options, FileAttributes attributes, DokanNet.IDokanFileInfo info)
		{
			string path = fileName;
				var pathData = getPathData(path);
			if (info.IsDirectory)
			{
				System.Console.WriteLine("create directory " + pathData.path);

			}
			else
            {
				System.Console.WriteLine("create File " + pathData.path);
			}
			if (pathData.parent_path != "root")
            {
                ShoFileInfo fileInfo = copyFileInfo(info);
				fileInfo.IsDirectory = true;

                this.CreateFile(pathData.parent_path, access, share, mode, options, attributes, fileInfo);
				pathData = getPathData(path);
			}

            if (pathData.pathEntry != null)
			{
				if (pathData.pathEntry.FileInfo.IsDirectory == true && info.IsDirectory == false)
				{
					throw new ArgumentException("Directory with same name already exists!");
				}
				else
				{
					return NtStatus.Success;
				}
			}

			FileSystemEntry entry = new FileSystemEntry();
			entry.Attributes = attributes;
			entry.FileName = pathData.name;
			entry.access = access;
			entry.LastAccessTime = DateTime.Now;
			entry.CreationTime = DateTime.Now;
			entry.LastWriteTime = DateTime.Now;
			entry.FullName = pathData.path;
			entry.FileInfo = copyFileInfo( info);
			entry.Length = 0;
			entry.options = options;
			entry.share = share;
			
				
				var ent = JsonConvert.SerializeObject(entry);
				using (var session = cluster.Connect(this.myDBData.KeySpace))
				{

					var qrCreate = create_path_id(pathData, session);

					qrCreate = session.Prepare("insert into meta (parent_id,id,entry) values (?,?,?) ;");
					session.Execute(qrCreate.Bind(pathData.parent_id, pathData.path_id, ent));


					return NtStatus.Success;
				}








			}

        private static ShoFileInfo copyFileInfo(IDokanFileInfo info)
        {
            ShoFileInfo fileInfo = new ShoFileInfo();
            //fileInfo.LongContext = (ulong)info.Context;
            fileInfo.DeleteOnClose = info.DeleteOnClose;
            fileInfo.IsDirectory = info.IsDirectory;
            fileInfo.NoCacheSet = info.NoCache;
            fileInfo.PagingIoSet = info.PagingIo;
            fileInfo.ProcessIdSet = info.ProcessId;
            fileInfo.Requester = info.GetRequestor();
            fileInfo.SynchronousIoSet = info.SynchronousIo;
            fileInfo.WriteToEndOfFileSet = info.WriteToEndOfFile;
            return fileInfo;
        }


        public static  PreparedStatement create_path_id(PathData myPath, ISession session)
        {
            PreparedStatement qrCreate = session.Prepare("insert into path_id (path,id) values (?,?);");
            myPath.path_id = System.Guid.NewGuid().ToString();
            session.Execute(qrCreate.Bind(myPath.path, myPath.path_id));

            qrCreate = session.Prepare("insert into id_path (id,path) values (?,?);");

            session.Execute(qrCreate.Bind(myPath.path_id, myPath.path));

            string[] indexArr = getIndexValues(myPath.path);

            foreach (var str in indexArr)
            {
				qrCreate = session.Prepare("insert into id_name (id,name) values (?,?);");

				session.Execute(qrCreate.Bind(myPath.path_id, str));


				qrCreate = session.Prepare("insert into name_id (name,id,parent_id) values (?,?,?);");

				session.Execute(qrCreate.Bind(str,myPath.path_id,myPath.parent_id));
			}




            return qrCreate;
        }

        public static string[] getIndexValues(string text)
        {
            Regex removeSpaces = new Regex("[\\s\\t\\n\\r]+");

            Regex regRemoveAllButLettersSpacesNumbers = new Regex("[^a-zA-Z0-9 ابتثجحخدذرزسشصضطظعغفقكلمنهويءئ]+");
            Regex alf = new Regex("[آأإ]");



            string indexStr = text;
            indexStr = indexStr.Trim();
            indexStr = alf.Replace(indexStr, "ا");
            indexStr = indexStr.Replace("ى", "ي");
            indexStr = indexStr.Replace("ة", "ه");
            indexStr = indexStr.Replace("٠", "0");
            indexStr = indexStr.Replace("١", "1");
            indexStr = indexStr.Replace("٢", "2");
            indexStr = indexStr.Replace("٣", "3");
            indexStr = indexStr.Replace("٤", "4");
            indexStr = indexStr.Replace("٥", "5");
            indexStr = indexStr.Replace("٦", "6");
            indexStr = indexStr.Replace("٧", "7");
            indexStr = indexStr.Replace("٨", "8");
            indexStr = indexStr.Replace("٩", "9");

            indexStr = regRemoveAllButLettersSpacesNumbers.Replace(indexStr, " ");

            indexStr = removeSpaces.Replace(indexStr, " ");

            string[] indexArr = indexStr.Split(new string[] { " " }, StringSplitOptions.RemoveEmptyEntries);
			indexArr = indexArr.Distinct().ToArray();
            return indexArr;
        }

		private void deletePathId(string path, string id, ISession session)
		{
			var qr = session.Prepare("delete from path_id where path = ? ;");
			session.Execute(qr.Bind(path));
			qr = session.Prepare("delete from id_path where id = ? ;");
			session.Execute(qr.Bind(id));

			qr = session.Prepare("select * from id_name where id = ? ;");
			var rows = session.Execute(qr.Bind(id));

			foreach (var row in rows)
            {
				qr = session.Prepare("delete from name_id where name = ? and id=? ;");
				session.Execute(qr.Bind(row.GetValue<string>("name"),id));
			}

			qr = session.Prepare("delete from id_name where id = ? ;");
			session.Execute(qr.Bind(id));



		}

		private void DetetePath(string fileName)
        {
			var path = fileName;
			var pathData = getPathData(path);

			System.Console.WriteLine("delete path " + pathData.path);


			if (string.IsNullOrWhiteSpace(pathData.path_id))
			{
				throw new FileNotFoundException();
			}

			List<FileSystemEntry> childs = this.ListEntriesInDirectory(pathData.path);

			foreach (var child in childs)
			{
				DetetePath(child.FullName);
			}

			using (var session = cluster.Connect(this.myDBData.KeySpace))
			{
				deletePathId(pathData.path, pathData.path_id, session);
				var qr = session.Prepare("delete from meta where parent_id = ? and id = ? ;");
				session.Execute(qr.Bind(pathData.parent_id, pathData.path_id));
				qr = session.Prepare("delete from chunk where id = ? ;");
				session.Execute(qr.Bind(pathData.path_id));
			}
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

			System.Console.WriteLine("list entries " + (parent_id == "root" ? "/" : pathData?.path));

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

						var entry = JsonConvert.DeserializeObject<FileSystemEntry>(row.GetValue<string>("entry"));
						entry.FullName = pathString;

						entries.Add(entry);

					}

				}

				return entries;
			}
		}



		public DokanNet.NtStatus DeleteDirectory(string fileName, DokanNet.IDokanFileInfo info)
		{
			this.DetetePath(fileName);
			return NtStatus.Success;
		}

		public DokanNet.NtStatus DeleteFile(string fileName, DokanNet.IDokanFileInfo info)
		{
			this.DetetePath(fileName);
			return NtStatus.Success;
		}

		public DokanNet.NtStatus FindFiles(string fileName, out IList<DokanNet.FileInformation> files, DokanNet.IDokanFileInfo info)
		{
			throw new NotImplementedException();
		}

		public DokanNet.NtStatus FindFilesWithPattern(string fileName, string searchPattern, out IList<DokanNet.FileInformation> files, DokanNet.IDokanFileInfo info)
		{
			throw new NotImplementedException();
		}

		public DokanNet.NtStatus FindStreams(string fileName, out IList<DokanNet.FileInformation> streams, DokanNet.IDokanFileInfo info)
		{
			throw new NotImplementedException();
		}

		public DokanNet.NtStatus FlushFileBuffers(string fileName, DokanNet.IDokanFileInfo info)
		{
			throw new NotImplementedException();
		}

		public DokanNet.NtStatus GetDiskFreeSpace(out long freeBytesAvailable, out long totalNumberOfBytes, out long totalNumberOfFreeBytes, DokanNet.IDokanFileInfo info)
		{
			freeBytesAvailable = _freeSpace;
			totalNumberOfBytes = _size;
			totalNumberOfFreeBytes = _freeSpace;
			return NtStatus.Success;
		}

		public DokanNet.NtStatus GetFileInformation(string fileName, out DokanNet.FileInformation fileInfo, DokanNet.IDokanFileInfo info)
		{
			throw new NotImplementedException();
		}

		public DokanNet.NtStatus GetFileSecurity(string fileName, out FileSystemSecurity security, AccessControlSections sections, DokanNet.IDokanFileInfo info)
		{
			throw new NotImplementedException();
		}

		public DokanNet.NtStatus GetVolumeInformation(out string volumeLabel, out DokanNet.FileSystemFeatures features, out string fileSystemName, out uint maximumComponentLength, DokanNet.IDokanFileInfo info)
		{
			throw new NotImplementedException();
		}

		public DokanNet.NtStatus LockFile(string fileName, long offset, long length, DokanNet.IDokanFileInfo info)
		{
			throw new NotImplementedException();
		}

		public DokanNet.NtStatus Mounted(DokanNet.IDokanFileInfo info)
		{
			throw new NotImplementedException();
		}

		public DokanNet.NtStatus MoveFile(string oldName, string newName, bool replace, DokanNet.IDokanFileInfo info)
		{
			throw new NotImplementedException();
		}

		public DokanNet.NtStatus ReadFile(string fileName, byte[] buffer, out int bytesRead, long offset, DokanNet.IDokanFileInfo info)
		{
			throw new NotImplementedException();
		}

		public DokanNet.NtStatus SetAllocationSize(string fileName, long length, DokanNet.IDokanFileInfo info)
		{
			throw new NotImplementedException();
		}

		public DokanNet.NtStatus SetEndOfFile(string fileName, long length, DokanNet.IDokanFileInfo info)
		{
			throw new NotImplementedException();
		}

		public DokanNet.NtStatus SetFileAttributes(string fileName, FileAttributes attributes, DokanNet.IDokanFileInfo info)
		{
			throw new NotImplementedException();
		}

		public DokanNet.NtStatus SetFileSecurity(string fileName, FileSystemSecurity security, AccessControlSections sections, DokanNet.IDokanFileInfo info)
		{
			throw new NotImplementedException();
		}

		public DokanNet.NtStatus SetFileTime(string fileName, DateTime? creationTime, DateTime? lastAccessTime, DateTime? lastWriteTime, DokanNet.IDokanFileInfo info)
		{
			throw new NotImplementedException();
		}

		public DokanNet.NtStatus UnlockFile(string fileName, long offset, long length, DokanNet.IDokanFileInfo info)
		{
			throw new NotImplementedException();
		}

		public DokanNet.NtStatus Unmounted(DokanNet.IDokanFileInfo info)
		{
			throw new NotImplementedException();
		}

		public DokanNet.NtStatus WriteFile(string fileName, byte[] buffer, out int bytesWritten, long offset, DokanNet.IDokanFileInfo info)
		{
			throw new NotImplementedException();
		}
	}

}
