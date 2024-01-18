-- schema for utility functions
create schema util;

-- checks if the text is non-empty after removing leading and trailing spaces
create or replace function util.text_non_empty_trimmed_text(val text) returns boolean as
$$
begin
    return trim(val) <> '';
end;
$$ language plpgsql;

-- checks if the text is null or non-empty after removing leading and trailing spaces
create or replace function util.text_null_or_non_empty_trimmed_text(val text) returns boolean as
$$
begin
    if val is null then
        return true;
    end if;

    return trim(val) <> '';
end;
$$ language plpgsql;

-- checks if the text array contains any non-empty text after removing leading and trailing spaces
create or replace function util.array_contains_non_empty_trimmed_text(val text[]) returns boolean as
$$
declare
    i int;
begin
    for i in array_lower(val, 1) .. coalesce(array_upper(val, 1), 0)
        loop
            if trim(val[i]) = '' then
                return false;
            end if;
        end loop;

    return true;
end;
$$ language plpgsql;

-- checks if the text array is null or contains any non-empty text after removing leading and trailing spaces
create or replace function util.array_null_or_contains_empty_trimmed_text(val text[]) returns boolean as
$$
declare
    i int;
begin
    if val is null then
        return true;
    end if;

    for i in array_lower(val, 1) .. coalesce(array_upper(val, 1), 0)
        loop
            if trim(val[i]) = '' then
                return false;
            end if;
        end loop;

    return true;
end;
$$ language plpgsql;

-- checks if text array values are unique
create or replace function util.array_text_values_unique(val text[])
    returns boolean as
$$
begin
    return array_length(val, 1) = array_length(array(select distinct unnest(val)), 1);
end;
$$ language plpgsql;

-- checks if null or text array values are unique
create or replace function util.array_null_or_text_values_unique(val text[])
    returns boolean as
$$
begin
    if val is null then
        return true;
    end if;

    return array_length(val, 1) = array_length(array(select distinct unnest(val)), 1);
end;
$$ language plpgsql;


-- checks if varchar array values are unique
create or replace function util.array_varchar_values_unique(val varchar[])
    returns boolean as
$$
begin
    return array_length(val, 1) = array_length(array(select distinct unnest(val)), 1);
end;
$$ language plpgsql;

-- checks if null or varchar array values are unique
create or replace function util.array_null_or_varchar_values_unique(val varchar[])
    returns boolean as
$$
begin
    if val is null then
        return true;
    end if;

    return array_length(val, 1) = array_length(array(select distinct unnest(val)), 1);
end;
$$ language plpgsql;

-- checks if integer array values are unique
create or replace function util.array_int_values_unique(val integer[])
    returns boolean as
$$
begin
    return array_length(val, 1) = array_length(array(select distinct unnest(val)), 1);
end;
$$ language plpgsql;

-- checks if null or integer array values are unique
create or replace function util.array_null_or_int_values_unique(val integer[])
    returns boolean as
$$
begin
    if val is null then
        return true;
    end if;

    return array_length(val, 1) = array_length(array(select distinct unnest(val)), 1);
end;
$$ language plpgsql;

-- checks if bigint array values are unique
create or replace function util.array_bigint_values_unique(val bigint[])
    returns boolean as
$$
begin
    return array_length(val, 1) = array_length(array(select distinct unnest(val)), 1);
end;
$$ language plpgsql;

-- checks if null or bigint array values are unique
create or replace function util.array_null_or_bigint_values_unique(val bigint[])
    returns boolean as
$$
begin
    if val is null then
        return true;
    end if;

    return array_length(val, 1) = array_length(array(select distinct unnest(val)), 1);
end;
$$ language plpgsql;


-- checks if any array values are unique
create or replace function util.array_values_unique(val anyarray)
    returns boolean as
$$
begin
    return array_length(val, 1) = array_length(array(select distinct unnest(val)), 1);
end;
$$ language plpgsql;

-- checks if null or any array values are unique
create or replace function util.array_null_or_values_unique(val anyarray)
    returns boolean as
$$
begin
    if val is null then
        return true;
    end if;

    return array_length(val, 1) = array_length(array(select distinct unnest(val)), 1);
end;
$$ language plpgsql;

-- check if integer is non 0 value
create or replace function util.integer_non_zero_integer(val integer)
    returns boolean as
$$
begin
    return val <> 0;
end
$$ language plpgsql;

-- check if integer is null or non 0 value
create or replace function util.integer_null_or_non_zero_integer(val integer)
    returns boolean as
$$
begin
    if val is null then
        return true;
    end if;

    return val <> 0;
end
$$ language plpgsql;

-- check if bigint is non 0 value
create or replace function util.bigint_non_zero_bigint(val bigint)
    returns boolean as
$$
begin
    return val <> 0;
end
$$ language plpgsql;

-- check if bigint is null or non 0 value
create or replace function util.bigint_null_or_non_zero_bigint(val bigint)
    returns boolean as
$$
begin
    if val is null then
        return true;
    end if;

    return val <> 0;
end
$$ language plpgsql;

-- sets the updated_at timestamp on a table on update
create or replace function util.set_updated_at()
    returns trigger as
$$
begin
    new.updated_at = now();
    return new;
end;
$$ language plpgsql;

-- sets the updated_at timestamp on a table on update when the new value is distinct from the old value
create or replace function util.set_updated_at()
    returns trigger as
$$
begin
    if new is distinct from old then
        new.updated_at = now();
    end if;

    return new;
end;
$$ language plpgsql;