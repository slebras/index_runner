

def get_upa_from_msg_data(msg_data):
    """Get the UPA workspace reference from a Kafka workspace event payload."""
    ws_id = msg_data.get('wsid')
    if not ws_id:
        raise RuntimeError(f'Event data missing the "wsid" field for workspace ID: {msg_data}')
    obj_id = msg_data.get('objid')
    if not obj_id:
        raise RuntimeError(f'Event data missing the "objid" field for object ID: {msg_data}')
    return f"{ws_id}/{obj_id}"
