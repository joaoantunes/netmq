using NetMQ;
using System;

namespace MajordomoProtocol
{
    public class Request : IEquatable<Request>
    {
        private TimeSpan m_timeout;

        private DateTime? m_lastSent;

        private int m_maxAttemptsAllowed;

        public string ServiceName { get; }
        public NetMQMessage Payload { get; }

        public Guid RequestId { get; private set; }

        public int TriedAttempts { get; private set; }

        public bool IsTimeout
        {
            get
            {
                return DateTime.UtcNow > (m_lastSent + m_timeout);
            }
        }

        public bool IsRepeatable
        {
            get
            {
                return TriedAttempts < m_maxAttemptsAllowed;
            }
        }

        public Request(string serviceName, NetMQMessage payload, TimeSpan? timeout = null, int maxAttemptsAllowed = 1)
        {
            if (String.IsNullOrEmpty(serviceName))
            {
                throw new ArgumentNullException("serviceName was not defined");
            }

            if (payload == null)
            {
                throw new ArgumentNullException("payload was not defined");
            }

            if (maxAttemptsAllowed < 1)
            {
                throw new ArgumentException("MaxAttemptsAllowed must be an number greater than 0");
            }

            this.ServiceName = serviceName;
            this.RequestId = Guid.NewGuid();

            payload.Append(this.RequestId.ToString());

            this.Payload = payload;
            this.m_maxAttemptsAllowed = maxAttemptsAllowed;
            this.m_timeout = timeout ?? TimeSpan.FromMilliseconds(5000);
        }

        public void UpdateAttempt()
        {
            TriedAttempts++;
            m_lastSent = DateTime.UtcNow;
        }

        public sealed override bool Equals(object other)
        {
            return other != null && other is Request && Equals((Request)other);
        }

        public sealed override int GetHashCode()
        {
            return this.RequestId.GetHashCode();
        }

        public bool Equals(Request other)
        {
            return other != null && this.RequestId.Equals(other.RequestId);
        }
    }
}
