create table operation(remote_addr, type, error, payload);

-- this gives us a mapping from any message field to the root rowid

create view message_apex_ids(field_id, top_id) as
with recursive parents(id, parent) as (
    select rowid, coalesce(parent_id,rowid) from messages
    union all
    select parents.id, parent_id from messages join parents on parent=messages.rowid
    -- order by rowid
)
select
    parents.*
    from parents
    join messages on parents.parent=messages.rowid
    where messages.parent_id is null;

-- show samples with the id of the node that gave them
select quote(id_value.value), quote(infohash) from samples_infohashes join messages as infohash_value_field on infohash_value_field.rowid=samples_infohashes.field_id join dict_items as id_dict_item on id_dict_item.dict_id=infohash_value_field.parent_id and cast(id_dict_item.key as text)='id' join messages as id_value on id_value.rowid=id_dict_item.value_id order by id_value.value, infohash;


CREATE VIEW dict_items as
select
    dict.rowid as dict_id,
    key.depth, key.value as key,
    value.rowid as value_id,
    value.value as value
from messages as dict
join messages as key on key.parent_id=dict.rowid and key."index"%2=0 and key.type='s'
join messages as value on value.parent_id=dict.rowid and value."index"=key."index"+1
where dict.type='d';

-- match sends with replies
select
    send.remote_addr, reply.remote_addr, quote(send_t.value), quote(reply_t.value), reply.type
from
    operations send,
    dict_items send_t on cast(send_t.key as text)='t' and send_t.depth=1,
    message_apex_ids send_t_apex on send_t_apex.field_id=send_t.value_id and send_t_apex.top_id=send.message_id,
    dict_items reply_t on reply_t.value=send_t.value,
    message_apex_ids reply_t_apex on reply_t_apex.field_id=reply_t.value_id,
    operations reply on reply.message_id=reply_t_apex.top_id and reply.type='recv'
where
    send.type='send';
