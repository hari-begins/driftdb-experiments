from driftdb.metrics.signature import normalize_plan_text, query_signature, raw_plan_digest


def test_normalizes_attribute_ids_and_literals():
    t1 = "Filter ((isnotnull(id#18L) AND (id#18L > 2000)) AND isnotnull(key#19L))"
    t2 = "Filter ((isnotnull(id#45L) AND (id#45L > 2050)) AND isnotnull(key#46L))"
    assert normalize_plan_text(t1) == normalize_plan_text(t2)
    assert query_signature(t1) == query_signature(t2)


def test_different_structure_gives_different_signature():
    t1 = "Filter (id#1L > 5)"
    t2 = "Filter (id#1L < 5)"
    assert query_signature(t1) != query_signature(t2)


def test_string_literals_normalized():
    t1 = "Filter (region#1 = 'east')"
    t2 = "Filter (region#2 = 'west')"
    assert query_signature(t1) == query_signature(t2)


def test_raw_digest_is_sensitive_to_everything():
    t1 = "Filter (id#1L > 5)"
    t2 = "Filter (id#2L > 5)"
    assert raw_plan_digest(t1) != raw_plan_digest(t2)
