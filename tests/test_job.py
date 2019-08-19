import pytest

from kong.model import Job
import peewee as pw

def test_create(tree):
    j1 = Job.create(batch_job_id=42, folder=tree, command="sleep 2", driver="LocalDriver")
    assert j1.job_id is not None
    job = Job.get(batch_job_id=42)
    assert job == j1
    assert len(tree.jobs) == 1
    assert tree.jobs[0] == j1
    assert j1.folder == tree
    assert j1.command == "sleep 2"

    with pytest.raises(pw.IntegrityError):
        Job.create(batch_job_id=42, command="sleep 4", folder=tree, driver="LocalDriver")
    assert Job.create(batch_job_id=43, folder=tree, command="sleep 4", driver="LocalDriver") is not None


    f2 = tree.subfolder("f2")
    j2 = Job.create(batch_job_id=44, folder=f2, command="sleep 4", driver="LocalDriver")
    assert j2 is not None
    assert j2.command == "sleep 4"
    assert len(f2.jobs) == 1
    assert f2.jobs[0] == j2
    assert j2.folder == f2
