# Reliable UDP Transfer

A Java client that sends files reliably over UDP. A short TCP handshake sets up the transfer, then the file goes over UDP in window sized bursts.

- Cumulative ACKs per burst, a middle ground between Stop and Wait and Go Back N
- Retransmission timer on its own thread with synchronized access to the shared window state
- Aborts cleanly if the server stops responding

`BurstyFtp.java` is my implementation. `FtpSegment.java` and `BurstyDriver.java` were written by Majid Ghaderi.

## Build and run

```bash
javac BurstyFtp.java FtpSegment.java BurstyDriver.java
java BurstyDriver -i myfile.txt -p 2025 -w 5
```

Needs a compatible receiving server (not included here).