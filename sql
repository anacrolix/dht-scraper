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
