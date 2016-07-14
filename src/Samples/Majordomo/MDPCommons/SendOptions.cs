using System;

namespace MDPCommons
{
    public class SendOptions
    {
        public TimeSpan Timeout { get; set; }

        public int Attempts { get; set; }

        public SendOptions()
        {
            this.Attempts = 1;
            this.Timeout = TimeSpan.FromSeconds(30);
        }
    }
}
