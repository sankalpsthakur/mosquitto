#!/usr/bin/env python3

# Restore must not trust payloadlen over the SQLite blob size (issue #3777).
# A mismatched payloadlen vs blob must be skipped without out-of-bounds reads.

from mosq_test_helper import *

persist_help = persist_module()

port, _ = mosq_test.get_port(2)
conf_file = os.path.basename(__file__).replace(".py", ".conf")
persist_help.write_config(
    conf_file,
    port,
    additional_config_entries={"plugin_opt_flush_period": 0, "log_type": "all"},
)

rc = 1
persist_help.init(port)

proto_ver = 5
qos = 0
good_topic = "test/good-retain"
oversized_topic = "test/oversized-retain"
null_topic = "test/null-blob-retain"
good_payload = b"good retained payload"
oversized_payload = b"oversized retained payload"
null_payload = b"null blob retained payload"

will_persist_id = "will-persist-client"
will_clean_id = "will-clean-client"
will_persist_topic = "test/will-persist"
will_clean_topic = "test/will-clean"
will_persist_payload = b"persist will payload"
will_clean_payload = b"clean will payload"

connect_packet = mqtt_packets.gen_connect(
    "persist-payloadlen-mismatch", proto_ver=proto_ver, clean_session=True
)
connack_packet = mqtt_packets.gen_connack(rc=0, proto_ver=proto_ver)

will_persist_connect = mqtt_packets.gen_connect(
    will_persist_id,
    proto_ver=proto_ver,
    clean_session=False,
    session_expiry=60,
    will_topic=will_persist_topic,
    will_payload=will_persist_payload,
    will_qos=0,
    will_retain=True,
)
will_clean_connect = mqtt_packets.gen_connect(
    will_clean_id,
    proto_ver=proto_ver,
    clean_session=True,
    session_expiry=0,
    will_topic=will_clean_topic,
    will_payload=will_clean_payload,
    will_qos=0,
    will_retain=True,
)

publish_good = mqtt_packets.gen_publish(
    good_topic, qos=qos, payload=good_payload, retain=True, proto_ver=proto_ver
)
publish_oversized = mqtt_packets.gen_publish(
    oversized_topic, qos=qos, payload=oversized_payload, retain=True, proto_ver=proto_ver
)
publish_null = mqtt_packets.gen_publish(
    null_topic, qos=qos, payload=null_payload, retain=True, proto_ver=proto_ver
)


def expect_no_retain(sock, topic, mid):
    subscribe_packet = mqtt_packets.gen_subscribe(mid, topic, 0, proto_ver=proto_ver)
    suback_packet = mqtt_packets.gen_suback(mid, qos=0, proto_ver=proto_ver)
    mosq_test.do_send_receive(sock, subscribe_packet, suback_packet, "suback " + topic)
    mosq_test.do_ping(sock)


broker = mosq_test.start_broker(filename=os.path.basename(__file__), use_conf=True, port=port)
will_persist_sock = None
will_clean_sock = None
stde = None

try:
    sock = mosq_test.do_client_connect(connect_packet, connack_packet, timeout=5, port=port)
    sock.send(publish_good)
    mosq_test.do_ping(sock)
    sock.send(publish_oversized)
    mosq_test.do_ping(sock)
    sock.send(publish_null)
    mosq_test.do_ping(sock)
    sock.close()

    will_persist_sock = mosq_test.do_client_connect(
        will_persist_connect, connack_packet, timeout=5, port=port
    )
    mosq_test.do_ping(will_persist_sock)
    will_clean_sock = mosq_test.do_client_connect(
        will_clean_connect, connack_packet, timeout=5, port=port
    )
    mosq_test.do_ping(will_clean_sock)

    persist_help.check_counts(
        port,
        clients=1,
        base_msgs=3,
        retain_msgs=3,
        wills=2,
    )

    # SIGKILL so wills remain in the DB instead of being published on shutdown.
    broker.kill()
    mosq_test.wait_for_subprocess(broker)
    broker = None
    will_persist_sock.close()
    will_clean_sock.close()
    will_persist_sock = None
    will_clean_sock = None

    with persist_help.get_connection(port, readonly=False) as con:
        cur = con.execute(
            "UPDATE base_msgs SET payload = ?, payloadlen = ? WHERE topic = ?",
            (b"X", 1048576, oversized_topic),
        )
        if cur.rowcount != 1:
            raise ValueError("failed to corrupt oversized retain row")
        cur = con.execute(
            "UPDATE base_msgs SET payload = NULL, payloadlen = ? WHERE topic = ?",
            (123, null_topic),
        )
        if cur.rowcount != 1:
            raise ValueError("failed to corrupt NULL-blob retain row")
        cur = con.execute(
            "UPDATE wills SET payload = ?, payloadlen = ? WHERE client_id = ?",
            (b"Y", 1048576, will_persist_id),
        )
        if cur.rowcount != 1:
            raise ValueError("failed to corrupt persistent will row")
        cur = con.execute(
            "UPDATE wills SET payload = NULL, payloadlen = ? WHERE client_id = ?",
            (999, will_clean_id),
        )
        if cur.rowcount != 1:
            raise ValueError("failed to corrupt clean-session will row")
        con.commit()

    broker = mosq_test.start_broker(filename=os.path.basename(__file__), use_conf=True, port=port)

    sock = mosq_test.do_client_connect(connect_packet, connack_packet, timeout=5, port=port)
    mid = 1
    subscribe_packet = mqtt_packets.gen_subscribe(mid, good_topic, 0, proto_ver=proto_ver)
    suback_packet = mqtt_packets.gen_suback(mid, qos=0, proto_ver=proto_ver)
    mosq_test.do_send_receive(sock, subscribe_packet, suback_packet, "suback good")
    mosq_test.expect_packet(sock, "good retain", publish_good)
    mosq_test.do_ping(sock)

    expect_no_retain(sock, oversized_topic, 2)
    expect_no_retain(sock, null_topic, 3)
    expect_no_retain(sock, will_persist_topic, 4)
    expect_no_retain(sock, will_clean_topic, 5)
    sock.close()

    (broker_terminate_rc, stde) = mosq_test.terminate_broker(broker)
    broker = None

    if "payloadlen" not in stde or "blob size" not in stde:
        raise ValueError("expected payloadlen vs blob size skip warning in broker log:\n%s" % (stde,))
    if "Restored 1 base messages (2 failed)" not in stde:
        raise ValueError("expected mismatched base messages to be skipped:\n%s" % (stde,))
    if "Restored 0 will messages (2 failed)" not in stde:
        raise ValueError("expected mismatched will messages to be skipped:\n%s" % (stde,))

    rc = broker_terminate_rc
except mosq_test.TestError:
    pass
except Exception as err:
    print(err)
    traceback.print_exc()
finally:
    if will_persist_sock is not None:
        will_persist_sock.close()
    if will_clean_sock is not None:
        will_clean_sock.close()
    if broker is not None:
        mosq_test.terminate_broker(broker)
        if mosq_test.wait_for_subprocess(broker):
            print("broker not terminated")
            if rc == 0:
                rc = 1
    os.remove(conf_file)
    rc += persist_help.cleanup(port)

    if rc:
        print(stde)
        print(mosq_test.broker_log(broker))


exit(rc)
