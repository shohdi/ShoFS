using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using ShoFSNameSpace.Services;
using SMBLibrary;
using SMBLibrary.Adapters;
using SMBLibrary.Authentication.GSSAPI;
using SMBLibrary.Authentication.NTLM;
using SMBLibrary.Server;

namespace ShoFSNameSpace
{
    class Program
    {
        static void Main(string[] args)
        {
            Thread thNew = new Thread(() => {
                Console.WriteLine("start test db!");
                ShoFS shohdi = new ShoFS("Shohdi File System", new List<string> { "127.0.0.1" }, "sa", "P@ssw0rd", "shohdi_file_system", null, null);

                shohdi.CreateDirectory("/share");
                shohdi.addAccess("/share", "Users", true, true);
                List<FileAccessModel> access = shohdi.getAccess("/share");
                FileSystemShare share = InitializeShare("/share", "/share", access.Where(u => u.read).Select(a => a.user).ToList(), access.Where(u => u.write).Select(a => a.user).ToList(), shohdi);
                IPAddress serverAddress = IPAddress.Parse("192.168.100.69");

                SMBTransportType transportType = SMBTransportType.DirectTCPTransport;
                GetUserPassword userPassword = new GetUserPassword((username) => username);
                NTLMAuthenticationProviderBase authenticationMechanism = new IndependentNTLMAuthenticationProvider(userPassword);
                SMBShareCollection shares = new SMBShareCollection();
                shares.Add(share);
                GSSProvider securityProvider = new GSSProvider(authenticationMechanism);
                SMBServer m_server = new SMBServer(shares, securityProvider);
                
                try
                {
                    m_server.Start(serverAddress, transportType);
                    if (transportType == SMBTransportType.NetBiosOverTCP)
                    {
                        if (serverAddress.AddressFamily == AddressFamily.InterNetwork && !IPAddress.Equals(serverAddress, IPAddress.Any))
                        {
                            IPAddress subnetMask = IPAddress.Parse("255.255.255.0");
                            NameServer m_nameServer = new NameServer(serverAddress, subnetMask);
                            m_nameServer.Start();
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                }
                while(true)
                {
                    Thread.Sleep(1);
                }
            });

            thNew.Start();
            thNew.Join();

            
        }

        public static FileSystemShare InitializeShare(string shareName,string sharePath,List<string> readAccess,List<string> writeAccess,ShoFS fileSystem)
        {
            
            FileSystemShare share = new FileSystemShare(shareName, new NTFileSystemAdapter(fileSystem));
            share.AccessRequested += delegate (object sender, AccessRequestArgs args)
            {
                bool hasReadAccess = Contains(readAccess, "Users") || Contains(readAccess, args.UserName);
                bool hasWriteAccess = Contains(writeAccess, "Users") || Contains(writeAccess, args.UserName);
                if (args.RequestedAccess == FileAccess.Read)
                {
                    args.Allow = hasReadAccess;
                }
                else if (args.RequestedAccess == FileAccess.Write)
                {
                    args.Allow = hasWriteAccess;
                }
                else // FileAccess.ReadWrite
                {
                    args.Allow = hasReadAccess && hasWriteAccess;
                }
            };
            return share;
        }

        public static bool Contains(List<string> list, string value)
        {
            return (IndexOf(list, value) >= 0);
        }

        public static int IndexOf(List<string> list, string value)
        {
            for (int index = 0; index < list.Count; index++)
            {
                if (string.Equals(list[index], value, StringComparison.OrdinalIgnoreCase))
                {
                    return index;
                }
            }
            return -1;
        }
    }


}
