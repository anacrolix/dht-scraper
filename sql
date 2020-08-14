-- this gives us a mapping from any message field to the root rowid

create view message_apex_ids as
with recursive parents(id, parent) as (
	select rowid, parent_id from messages
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
