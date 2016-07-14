using System;
using System.Text;

using NetMQ;

using MDPCommons;
using NetMQ.Sockets;
using JetBrains.Annotations;
using System.Collections.Generic;

namespace MajordomoProtocol
{
    /// <summary>
    ///     implements a client skeleton for Majordomo Protocol V0.1
    /// </summary>
    public class MDPClientAsync : IMDPClientAsync
    {
        private readonly TimeSpan m_defaultTimeOutPerRequest = TimeSpan.FromSeconds(30);
        private readonly TimeSpan m_defaultTimeOut = TimeSpan.FromMilliseconds(20000); // default value to be used to know if broker is not responding
        private readonly TimeSpan m_lingerTime = TimeSpan.FromMilliseconds(1);
        private readonly string m_mdpClient = MDPConstants.MDP_CLIENT_HEADER; //MDPConstants.MDP_CLIENT_ASYNC_HEADER;

        private NetMQSocket m_client;           // the socket to communicate with the broker
        private NetMQPoller m_poller;           // used to poll asynchronously the replies and possible timeouts

        private readonly string[] m_brokerAddresses;
        private int m_currentBrokerIndex;
        private readonly byte[] m_identity;
        private bool m_connected;               // used as flag true if a connection has been made
        private string m_serviceName;           // need that as storage for the event handler

        private NetMQTimer m_timer;

        private DateTime? m_lastReceivedRequest;

        private RequestsQueue m_requestQueue;

        private TimeSpan m_purgeCheckTime = TimeSpan.FromMilliseconds(30000);

        /// <summary>
        ///     returns the address of the broker the client is connected to
        /// </summary>
        public string Address => m_brokerAddresses[m_currentBrokerIndex];

        /// <summary>
        ///     returns the name of the client
        /// </summary>
        public byte[] Identity => m_identity;

        /// <summary>
        ///     if client has a log message available it fires this event
        /// </summary>
        public event EventHandler<MDPLogEventArgs> LogInfoReady;

        /// <summary>
        ///     if client has a reply message available it fires this event
        /// </summary>
        public event EventHandler<MDPReplyEventArgs> ReplyReady;

        public event EventHandler<MDPFailedRequestEventArgs> FailedRequest;

        /// <summary>
        ///     sets or gets the timeout period that a client can stay connected 
        ///     without receiving any messages from broker
        /// </summary>
        public TimeSpan Timeout { get; set; }

        /// <summary>
        ///     setup the client with standard values
        ///     verbose == false
        ///     Connect the client to broker
        /// </summary>
        private MDPClientAsync()
        {
            m_client = null;
            m_connected = false;
            m_currentBrokerIndex = -1; // TODO use random!?
            m_requestQueue = new RequestsQueue();
            m_requestQueue.FailedRequest += (s, e) => OnFailedRequest(e);

            m_poller = new NetMQPoller();
            Timeout = m_defaultTimeOut;

            m_timer = new NetMQTimer(m_purgeCheckTime);
            m_timer.Enable = false;
            m_timer.Elapsed += (s, e) => OnPurgeRequest();
            m_poller.Add(m_timer);

            m_poller.Add(m_timer);
            m_poller.RunAsync();
        }

        /// <summary>
        ///     setup the client, use standard values and parameters and connects client to broker
        /// </summary>
        /// <param name="brokerAddress">address the broker can be connected to</param>
        /// <param name="identity">if present will become the name for the client socket, encoded in UTF8</param>
        public MDPClientAsync(string[] brokerAddresses, byte[] identity = null)
            : this()
        {
            if (brokerAddresses != null && brokerAddresses.Length < 0)
                throw new ArgumentException(nameof(brokerAddresses), "At least one broker address must be defined");

            m_identity = identity;
            m_brokerAddresses = brokerAddresses;
        }

        /// <summary>
        ///     setup the client, use standard values and parameters and connects client to broker
        /// </summary>
        /// <param name="brokerAddress">address the broker can be connected to</param>
        /// <param name="identity">sets the name of the client (must be UTF8), if empty or white space it is ignored</param>
        public MDPClientAsync([NotNull] string[] brokerAddresses, string identity)
            : this()
        {
            if (brokerAddresses != null && brokerAddresses.Length < 0)
                throw new ArgumentException(nameof(brokerAddresses), "At least one broker address must be defined");

            if (!string.IsNullOrWhiteSpace(identity))
                m_identity = Encoding.UTF8.GetBytes(identity);

            m_brokerAddresses = brokerAddresses;
        }

        /// <summary>
        ///     send a asyncronous request to a broker for a specific service without receiving a reply
        ///     
        ///     there is no retry logic
        ///     according to the verbose flag it reports about its activities
        /// </summary>
        /// <param name="serviceName">the name of the service requested</param>
        /// <param name="request">the request message to process by service</param>
        /// <exception cref="ApplicationException">serviceName must not be empty or null.</exception>
        /// <exception cref="ApplicationException">the request must not be null</exception>
        public Guid Send([NotNull] string serviceName, NetMQMessage request, SendOptions options = null)
        {
            if (string.IsNullOrWhiteSpace(serviceName))
                throw new ApplicationException("serviceName must not be empty or null.");

            if (ReferenceEquals(request, null))
                throw new ApplicationException("the request must not be null");

            if (ReferenceEquals(options, null))
            {
                options.Timeout = m_defaultTimeOutPerRequest;
                options.Attempts = 1;
            }
                
            // memorize it for the event handler
            m_serviceName = serviceName; // TODO this is wrong because I can be sending multiple serviceNames

            // if for any reason the socket is NOT connected -> connect it!
            if (!m_connected)
                Connect();

            var message = new NetMQMessage(request);
            // prefix the request according to MDP specs
            // Frame 1: Empty Frame, because DEALER socket needs to emulate a REQUEST socket to comunicate with ROUTER socket
            // Frame 2: "MDPCxy" (six bytes MDP/Client x.y)
            // Frame 3: service name as printable string
            // Frame 4: request

            message.Push(serviceName);
            message.Push(m_mdpClient);
            message.PushEmptyFrame();

            var req = new Request(serviceName, message, options.Timeout, options.Attempts);
            var reqId = m_requestQueue.EnqueueRequest(req);
            Log($"[CLIENT INFO] sending requestId: {reqId} message: {message} to service {serviceName}");
            return reqId;
        }

        /// <summary>
        ///     broadcast the logging information if someone is listening
        /// </summary>
        /// <param name="e"></param>
        protected virtual void OnLogInfoReady(MDPLogEventArgs e)
        {
            LogInfoReady?.Invoke(this, e);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposing)
                return;

            if (!ReferenceEquals(m_poller, null))
                m_poller.Dispose();

            if (!ReferenceEquals(m_client, null))
                m_client.Dispose();
        }

        /// <summary>
        ///     connects to the broker, if a socket already exists it will be disposed and
        ///     a new socket created and connected (Linger is set to 1)
        ///     The Client connects to broker using a DEALER socket
        /// </summary>
        private void Connect()
        {
            if (!ReferenceEquals(m_client, null))
            {
                m_poller.Remove(m_client);
                m_client.Dispose(); // !? isto pode dar problemas!? por não estar no mm thread que o Poller!?
            }

            m_client = new DealerSocket();
            // sets a low linger to avoid port exhaustion, messages can be lost since it'll be sent again
            m_client.Options.Linger = m_lingerTime;

            if (m_identity != null)
                m_client.Options.Identity = m_identity;

            // set timeout timer to reconnect if no message is received during timeout
            m_lastReceivedRequest = DateTime.UtcNow;
            m_timer.EnableAndReset();

            // attach the event handler for incoming messages
            m_client.ReceiveReady += OnReceiveReady;
            m_client.SendReady += OnSendReady;
            m_poller.Add(m_client);

            RotateBroker();
            m_client.Connect(Address);

            m_connected = true;

            Log($"[CLIENT]: {m_client.Options.Identity} connecting to broker at {Address}");
        }

        private void OnSendReady(object sender, NetMQSocketEventArgs e)
        {
            Request request;
            var counter = 0;
            do
            {
                request = m_requestQueue.DequeueRequest();
                if (request == null)
                {
                    return; // There are no more requests to be sent!
                    // hmmm o que acontece se tiver um send ready e não tiver nada para enviar? vou ser sinalizado novamente!?
                    // se não for tou lixado... porque vou ter mais tarde pedidos e não vai enviar...
                }
                counter++;
            }
            while (m_client.TrySendMultipartMessage(request.Payload) && counter < 100); // need to be careful with sending starvation!?
            // TODO quando falhar... tenho de voltar a repor o pedido (ou posso esperar ele "fingir" que foi enviado e mais tarde vai dar timeout)
        }

        /// <summary>
        ///     handle the incoming messages
        ///     if a message is received timeout timer is reseted
        /// </summary>
        /// <remarks>
        ///     socket strips [client adr][e] from message // TODO remove this?!
        ///     message -> 
        ///                [empty frame][protocol header][service name][reply]
        ///                [empty frame][protocol header][service name][result code of service lookup]
        /// </remarks>
        private void OnReceiveReady(object sender, NetMQSocketEventArgs e)
        {
            Exception exception = null;
            NetMQMessage reply = null;
            Request req = null;
            Guid requestId = Guid.Empty;

            try
            {
                reply = m_client.ReceiveMultipartMessage();
                if (ReferenceEquals(reply, null))
                    throw new ApplicationException("Unexpected behavior");

                m_lastReceivedRequest = DateTime.UtcNow;

                Log($"[CLIENT INFO] received the reply {reply}");

                requestId = ExtractRequest(reply);
                req = m_requestQueue.ConcludeRequest(requestId);
            }
            catch (Exception ex)
            {
                exception = ex;
            }
            finally
            {
                if (req == null)
                {
                    Log($"[CLIENT INFO] RequestId: {requestId} processed in DUPLICATE!");
                }
                else
                {
                    ReturnReply(req, reply, exception);
                }
            }
        }

        /// <summary>
        ///   verifies if the message replied obeys the MDP 0.2 protocol
        /// </summary>
        /// <remarks>
        ///     socket strips [client adr][e] from message
        ///     message -> 
        ///                [empty frame][protocol header][service name][requestId][reply]
        ///                [empty frame][protocol header][service name][result code of service lookup]
        /// </remarks>
        private Guid ExtractRequest(NetMQMessage reply)
        {
            if (reply.FrameCount < 4) // TODO Check if I need to change to 5 because of reqId!
                throw new ApplicationException("[CLIENT ERROR] received a malformed reply");

            var emptyFrame = reply.Pop();
            if (emptyFrame != NetMQFrame.Empty)
            {
                throw new ApplicationException($"[CLIENT ERROR] received a malformed reply expected empty frame instead of: { emptyFrame } ");
            }
            var header = reply.Pop();

            if (header.ConvertToString() != m_mdpClient)
                throw new ApplicationException($"[CLIENT INFO] MDP Version mismatch: {header}");

            var service = reply.Pop();

            if (service.ConvertToString() != m_serviceName)
                throw new ApplicationException($"[CLIENT INFO] answered by wrong service: {service.ConvertToString()}");

            Guid requestId;  // TODO: Not sure if requestId should be the last frame or the request itself...
            var reqIdFrame = reply.Last;
            reply.RemoveFrame(reqIdFrame);
            if (!Guid.TryParse(reqIdFrame.ConvertToString(), out requestId) || requestId == Guid.Empty)
            {
                throw new ApplicationException($"[CLIENT INFO] RequestID was not retrieved");
            }
            return requestId;
        }

        private void OnPurgeRequest()
        {
            Log($"[CLIENT INFO] Going to purge requests");
            m_requestQueue.PurgeRequests();

            if (IsReconnectRequired())
            {
                Log($"[CLIENT INFO] No message received from broker during {Timeout} time");
                Connect();
            }
        }

        private bool IsReconnectRequired()
        {
            return DateTime.UtcNow > m_lastReceivedRequest + Timeout && m_requestQueue.ExistsOutstandingRequest();
        }

        private void RotateBroker()
        {
            if (m_currentBrokerIndex < 0)
            {
                m_currentBrokerIndex = 0; // TODO random!
            }
            else
            {
                m_currentBrokerIndex++;
                if (m_currentBrokerIndex >= m_brokerAddresses.Length)
                {
                    m_currentBrokerIndex = 0;
                }
            }
        }

        /// <summary>
        ///     broadcast the logging information if someone is listening
        /// </summary>
        /// <param name="e"></param>
        protected virtual void OnReplyReady(MDPReplyEventArgs e)
        {
            ReplyReady?.Invoke(this, e);
        }

        protected virtual void OnFailedRequest(MDPFailedRequestEventArgs e)
        {
            Log($"[CLIENT INFO] Request failed {e.Request}");
            FailedRequest?.Invoke(this, e);
        }

        private void ReturnReply(Request request, NetMQMessage reply, Exception exception = null)
        {
            OnReplyReady(new MDPReplyEventArgs(request.RequestId, request.ServiceName, reply, exception));
        }

        private void Log(string info)
        {
            if (!string.IsNullOrWhiteSpace(info))
                OnLogInfoReady(new MDPLogEventArgs { Info = info });
        }
    }
}
