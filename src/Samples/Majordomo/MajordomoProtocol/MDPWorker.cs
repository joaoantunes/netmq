using System;
using System.Threading;

using NetMQ;

using MDPCommons;
using NetMQ.Sockets;
using System.Threading.Tasks;

namespace MajordomoProtocol
{
    public class MDPWorker : IMDPWorker
    {
        // Majordomo protocol header
        private readonly string m_mdpWorker = MDPConstants.MDP_WORKER_HEADER;

        private const int _heartbeat_liveliness = 3;// indicates the remaining "live" for the worker

        private readonly string m_brokerAddress;    // the broker address to connect to
        private readonly string m_serviceName;      // the name of the service the worker offers
        private NetMQSocket m_worker;               // the worker socket itself -MDP requires to use DEALER
        private DateTime m_heartbeatAt;             // when to send HEARTBEAT
        private int m_liveliness;                   // how many attempts are left
        private NetMQFrame m_returnIdentity;        // the return identity if any
        private NetMQFrame m_requestId;
        private readonly int m_connectionRetries;   // the number of times the worker tries to connect to the broker before it abandons
        private int m_retriesLeft;                  // indicates the number of connection retries that are left
        private readonly byte[] m_identity;         // if not null the identity of the worker socket

        private NetMQQueue<Action> m_pollerQueue;
        private NetMQPoller m_poller;
        private NetMQTimer m_timer;
        private bool m_receivedMessage;


        public event EventHandler<NetMQMessage> Work;

        /// <summary>
        ///     send a heartbeat every specified milliseconds
        /// </summary>
        public TimeSpan HeartbeatDelay { get; set; }

        /// <summary>
        ///     delay in milliseconds between reconnects
        /// </summary>
        public TimeSpan ReconnectDelay { get; set; }

        /// <summary>
        ///     broadcast logging information via this event
        /// </summary>
        public event EventHandler<MDPLogEventArgs> LogInfoReady;

        
        /// <summary>
        ///     create worker with standard parameter
        ///     HeartbeatDelay == 2500 milliseconds
        ///     ReconnectDelay == 2500 milliseconds
        ///     ConnectionRetries == 3
        ///     Verbose == false
        /// </summary>
        private MDPWorker()
        {
            HeartbeatDelay = TimeSpan.FromMilliseconds(2500);
            ReconnectDelay = TimeSpan.FromMilliseconds(2500);

            m_timer = new NetMQTimer(HeartbeatDelay);
            m_timer.Enable = false;
            m_timer.Elapsed += (s, e) => OnHeartbeat();

            m_pollerQueue = new NetMQQueue<Action>();
            m_pollerQueue.ReceiveReady += (sender, args) =>
            {
                var action = args.Queue.Dequeue();
                action.Invoke();
            };

            m_poller = new NetMQPoller();
            m_poller.Add(m_pollerQueue);
            m_poller.Add(m_timer);
            m_poller.RunAsync();
        }

        /// <summary>
        ///     creates worker with standard parameter and
        ///     set the broker's address and the service name for the worker
        /// </summary>
        /// <param name="brokerAddress">the address the worker can connect to the broker at</param>
        /// <param name="serviceName">the service the worker offers</param>
        /// <param name="identity">the identity of the worker, if present - set automatic otherwise</param>
        /// <param name="connectionRetries">the number of times the worker tries to connect to the broker - default 3</param>
        /// <exception cref="ArgumentNullException">The address of the broker must not be null, empty or whitespace!</exception>
        /// <exception cref="ArgumentNullException">The name of the service must not be null, empty or whitespace!</exception>
        /// <remarks>
        ///     create worker with standard parameter
        ///     HeartbeatDelay == 2500 milliseconds
        ///     ReconnectDelay == 2500 milliseconds
        ///     Verbose == false
        ///
        ///     the worker will try to connect <c>connectionRetries</c> cycles to establish a
        ///     connection to the broker, within each cycle he tries it 3 times with <c>ReconnectDelay</c>
        ///     delay between each cycle
        /// </remarks>
        public MDPWorker(string brokerAddress, string serviceName, byte[] identity = null, int connectionRetries = 3)
            : this()
        {
            if (string.IsNullOrWhiteSpace(brokerAddress))
                throw new ArgumentNullException(nameof(brokerAddress),
                                                 "The address of the broker must not be null, empty or whitespace!");

            if (string.IsNullOrWhiteSpace(serviceName))
                throw new ArgumentNullException(nameof(serviceName),
                                                 "The name of the service must not be null, empty or whitespace!");
            m_identity = identity;
            m_brokerAddress = brokerAddress;
            m_serviceName = serviceName;
            m_connectionRetries = connectionRetries;

            Connect();
        }

        public void SendAck(NetMQMessage reply)
        {
            if (ReferenceEquals(reply, null))
            {
                throw new ArgumentNullException("reply");
            }

            if (ReferenceEquals(m_returnIdentity, null))
            {
                throw new Exception("You can only send a message after receiving a reply!");
            }

            var message = Wrap(reply, m_returnIdentity);
            if (m_requestId != null)
            {
                message.Append(m_requestId);
            }
            m_returnIdentity = null;
            m_requestId = null;
            m_pollerQueue.Enqueue(() => Send(MDPCommand.Reply, null, message));
        }

        /// <summary>
        ///     if a logging information shall be broadcasted
        ///     raise the event if someone is listening
        /// </summary>
        /// <param name="e">the wrapped logging information</param>
        protected virtual void OnLogInfoReady(MDPLogEventArgs e)
        {
            LogInfoReady?.Invoke(this, e);
        }

        /// <summary>
        ///     upon arrival of a message process it
        ///     and set the request variable accordingly
        /// </summary>
        /// <remarks>
        ///     worker expects to receive either of the following
        ///     REQUEST     -> [e][header][command][client adr][e][request]
        ///     HEARTBEAT   -> [e][header][command]
        ///     DISCONNECT  -> [e][header][command]
        ///     KILL        -> [e][header][command]
        /// </remarks>
        protected virtual void ProcessReceiveReady(object sender, NetMQSocketEventArgs e)
        {
            // a request has arrived process it
            var request = m_worker.ReceiveMultipartMessage();
            m_receivedMessage = true;

            Log($"[WORKER] received {request}");

            // any message from broker is treated as heartbeat(!) since communication exists
            m_liveliness = _heartbeat_liveliness;
            // check the expected message envelope and get the embedded MPD command
            var command = GetMDPCommand(request);
            // MDP command is one byte!
            switch (command)
            {
                case MDPCommand.Request:
                    // the message is [client adr][e][request]
                    // save as many addresses as there are until we hit an empty frame
                    // - for simplicity assume it is just one
                    m_returnIdentity = Unwrap(request); // TODO give this responsibility to outside?!
                    if (request.FrameCount == 2) // TODO Improve this definition!! Maybe use another request type!? RequestCorrelated
                    {
                        m_requestId = request.Last;
                        request.RemoveFrame(m_requestId);
                    }
                    else
                    {
                        m_requestId = null;
                    }
                    OnWork(request);
                    break;
                case MDPCommand.Heartbeat:
                    // reset the liveliness of the broker
                    m_liveliness = _heartbeat_liveliness;
                    break;
                case MDPCommand.Disconnect:
                    // reconnect the worker
                    Connect();
                    break;
                case MDPCommand.Kill:
                    // stop working you worker you
                    // m_exit = true; // TODO!
                    break;
                default:
                    Log("[WORKER ERROR] invalid command received!");
                    break;
            }
        }

        private void OnWork(NetMQMessage message)
        {
            Work?.Invoke(this, message);
        }

        /// <summary>
        /// Check the message envelope for errors
        /// and get the MDP Command embedded.
        /// The message will be altered!
        /// </summary>
        /// <param name="request">NetMQMessage received</param>
        /// <returns>the received MDP Command</returns>
        private MDPCommand GetMDPCommand(NetMQMessage request)
        {
            // don't try to handle errors
            if (request.FrameCount < 3)
                throw new ApplicationException("Malformed request received!");

            var empty = request.Pop();

            if (!empty.IsEmpty)
                throw new ApplicationException("First frame must be an empty frame!");

            var header = request.Pop();

            if (header.ConvertToString() != m_mdpWorker)
                throw new ApplicationException("Invalid protocol header received!");

            var cmd = request.Pop();

            if (cmd.BufferSize > 1)
                throw new ApplicationException("MDP Command must be one byte not multiple!");

            var command = (MDPCommand)cmd.Buffer[0];

            Log($"[WORKER] received {request}/{command}");

            return command;
        }

        /// <summary>
        /// Connect or re-connect to the broker.
        /// </summary>
        private void Connect()
        {
            // if the socket exists dispose it and re-create one
            if (!ReferenceEquals(m_worker, null))
            {
                DisposeWorker();
            }

            m_worker = new DealerSocket();
            // set identity if provided
            if (m_identity != null && m_identity.Length > 0)
                m_worker.Options.Identity = m_identity;

            m_timer.Interval = (int)HeartbeatDelay.TotalMilliseconds;
            m_timer.EnableAndReset();

            // hook up the received message processing method before the socket is connected
            m_worker.ReceiveReady += ProcessReceiveReady;
            m_poller.Add(m_worker);

            m_worker.Connect(m_brokerAddress);

            Log($"[WORKER] connected to broker at {m_brokerAddress}");

            // send READY to broker since worker is connected
            m_pollerQueue.Enqueue(() => Send(MDPCommand.Ready, m_serviceName, null));
            // reset liveliness to active broker
            m_liveliness = _heartbeat_liveliness;
            // set point in time for next heatbeat
            m_heartbeatAt = DateTime.UtcNow + HeartbeatDelay;
        }

        private void DisposeWorker()
        {
            var old = m_worker;
            old.ReceiveReady -= ProcessReceiveReady;
            m_poller.Remove(old);
            m_pollerQueue.Enqueue(() => old.Dispose());
        }

        private async void OnHeartbeat()
        {
            m_retriesLeft = m_connectionRetries; // TODO!?

            if (!m_receivedMessage)
            {
                if (--m_liveliness == 0)
                {
                    // if we tried it _HEARTBEAT_LIVELINESS * m_connectionRetries times without
                    // success therefor we deem the broker dead or the communication broken
                    // and abandon the worker
                    if (--m_retriesLeft < 0) // TODO!?
                    {
                        Log("[WORKER] abandoning worker due to errors!");
                        return;
                    }

                    Log("[WORKER INFO] disconnected from broker - retrying ...");

                    // wait before reconnecting
                    await Task.Delay(HeartbeatDelay);
                    // reconnect
                    Connect();
                }
            }
            else
            {
                m_receivedMessage = false; // reset receivedMessage
            }

            // if it is time to send a heartbeat to broker
            if (DateTime.UtcNow >= m_heartbeatAt) // TODO If I'm busy 2 Broker, There is no need to send heartbeats, because im not going to be purged!
            {
                m_pollerQueue.Enqueue(() => Send(MDPCommand.Heartbeat, null, null));
                // set new point in time for sending the next heartbeat
                m_heartbeatAt = DateTime.UtcNow + HeartbeatDelay;
            }
        }

        /// <summary>
        /// Send a message to broker
        /// if no message provided create a new empty one
        /// prepend the message with the MDP prologue
        /// </summary>
        /// <param name="mdpCommand">MDP command</param>
        /// <param name="data">data to be sent</param>
        /// <param name="message">the message to send</param>
        private void Send(MDPCommand mdpCommand, string data, NetMQMessage message)
        {
            // cmd, null, message      -> [REPLY],<null>,[client adr][e][reply]
            // cmd, string, null       -> [READY],[service name]
            // cmd, null, null         -> [HEARTBEAT]
            var msg = ReferenceEquals(message, null) ? new NetMQMessage() : message;
            // protocol envelope according to MDP
            // last frame is the data if available
            if (!ReferenceEquals(data, null))
            {
                // data could be multiple whitespaces or even empty(!)
                msg.Push(data);
            }
            // set MDP command                          ([client adr][e][reply] OR [service]) => [data]
            msg.Push(new[] { (byte)mdpCommand });     // [command][header][data]
            // set MDP Header
            msg.Push(m_mdpWorker);                     // [header][data]
            // set MDP empty frame as separator
            msg.Push(NetMQFrame.Empty);                // [e][command][header][data]

            Log($"[WORKER] sending {msg} to broker / Command {mdpCommand}");

            m_worker.SendMultipartMessage(msg);
        }

        /// <summary>
        /// Prepend the message with an empty frame as separator and a frame
        /// </summary>
        /// <returns>new message with wrapped content</returns>
        private static NetMQMessage Wrap(NetMQMessage msg, NetMQFrame frame)
        {
            var result = new NetMQMessage(msg);

            result.Push(NetMQFrame.Empty);            // according to MDP an empty frame is the separator
            result.Push(frame);                       // the return address

            return result;
        }

        /// <summary>
        /// Strip the message from the first frame and if empty the following frame as well
        /// </summary>
        /// <returns>the first frame of the message</returns>
        private static NetMQFrame Unwrap(NetMQMessage msg)
        {
            var result = msg.Pop();

            if (msg.First.IsEmpty)
                msg.Pop();

            return result;
        }

        /// <summary>
        /// Log the info via registered event handler.
        /// </summary>
        private void Log(string info)
        {
            if (!string.IsNullOrWhiteSpace(info))
                OnLogInfoReady(new MDPLogEventArgs { Info = info });
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

            if (!ReferenceEquals(m_worker, null))
            {
                DisposeWorker();
            }

            if (!ReferenceEquals(m_pollerQueue, null))
            {
                m_poller.Remove(m_pollerQueue);
                m_pollerQueue.Dispose();
            }
                
            if (!ReferenceEquals(m_poller, null))
                m_poller.Dispose();
        }
    }
}
