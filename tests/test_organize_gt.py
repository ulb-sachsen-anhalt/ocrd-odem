"""Test GT organization logics"""

import datetime as dt
import os
import shutil

from pathlib import (
    Path
)

import pytest

from scripts.organize_groundtruth import (
    create_next_groundtruth_batch,
    inspect,
    advance,
    _read_data,
    _write_data,
    _filter_project_img,
    _stage_from_path,
    HEADER_PACKAGE,
    HEADER_TODO,
    HEADER_DONE,
    HEADER_REWV,
    HEADER_STRC,
    GT_LABEL,
    GT_DATA_FIELDS,
    DIR_DONE,
    DIR_REVIEWED,
    DIR_HEADER_MAP,
    HEADER_DIR_MAP,
    UNSET,
    GT_DOMAIN,
)

from .conftest import (
    TEST_RES
)


_LABEL_SAMPLE_INSPECT_ = f'{GT_LABEL}_sample_inspect.csv'
_LABEL_SAMPLE_ = f'{GT_LABEL}_sample.csv'
_PATH_RES_SAMPLE_ = TEST_RES / _LABEL_SAMPLE_
_PROD_RES_ = Path(os.path.abspath(__file__)).parents[1] / 'resources'
_LABEL_PROD_FILE_ = 'odem_groundtruth_4185.csv'
_PATH_PROD_ = _PROD_RES_ / f'{GT_LABEL}.csv'
_TODAY_ = dt.datetime.today().strftime('%Y-%m-%d')
_FIRST_ORDER_ = '-4185-01'
_DUMMY_OCR_ = '<xml/>'
_DUMMY_JPG_ = b'\xff\xd8\xff\xd9'
# maybe JPG as JIFF ?
# _DUMMY_JPG_ = b'\x4A\x46\x49\x46'
URN_NO_01 = 'urn+nbn+de+gbv+3+1-138408-p0042-0_lat'
URN_NO_02 = 'urn+nbn+de+gbv+3+1-182075-p0034-1_ger'
URN_NO_03 = 'urn+nbn+de+gbv+3+1-137009-p0597-2_lat'
URN_NO_04 = 'urn+nbn+de+gbv+3+1-142331-p0182-2_lat+ger'


def _generate_fs_structs(root_path):
    "Util func to create plain top filesystem layout"
   # generate filesys struct matching GT initial source from 12/2021
    _odem_gt_root = root_path / 'OCR' / 'OCR_ODEM' / 'QA-Groundtruth'
    _odem_gt_root.mkdir(parents=True)
    (_odem_gt_root / GT_LABEL / 'MAX').mkdir(parents=True, exist_ok=True)
    (_odem_gt_root / GT_LABEL / 'FULLTEXT').mkdir(parents=True, exist_ok=True)
    # generate filesys struct matching NFS-Share
    _ocr_data_root = root_path / 'data' / 'ocr'
    _ocr_data_root.mkdir(parents=True)
    _ocr_share_gt = _ocr_data_root / 'groundtruth' / 'odem'
    _ocr_share_gt.mkdir(parents=True)
    _ocr_share_img = _ocr_data_root / 'media' / 'jpg' / 'odem'
    _ocr_share_img.mkdir(exist_ok=True, parents=True)
    return (_odem_gt_root, _ocr_data_root)


def _generate_win_network_struct_from(path_data_file, win_drive: Path):
    """Util func to generate and fill filesystem structs
    fitting layout on network drive mludata1.xd.uni-halle.de/OCR/ODEM"""

    _gt_dict = _read_data(path_data_file)
    _generated_paths = []
    for _d, _r in _gt_dict.items():
        # first, create original data source where
        # was ocr of old before any corrections done
        # TODO do we need this? only for fresh batches
        _path_gt_dir = win_drive.parent / GT_LABEL / 'FULLTEXT'
        _path_gt_dir.mkdir(exist_ok=True, parents=True)
        open(_path_gt_dir / (_d + '.xml'), mode='w').write(_DUMMY_OCR_)
        _path_img_dir = win_drive.parent / GT_LABEL / 'MAX'
        _path_img_dir.mkdir(exist_ok=True, parents=True)
        _path_img = _path_img_dir / (_d + '.jpg')
        open(_path_img, mode='wb').write(_DUMMY_JPG_)
        # now, probabely record has advanced further
        # => toDo, done, reviewed, etc
        for _sub_dir, _stage in DIR_HEADER_MAP.items():
            _curr_state = _r[_stage]
            if _curr_state != UNSET:
                _stage = DIR_HEADER_MAP[_sub_dir]
                _pkg_label = _TODAY_ + _FIRST_ORDER_
                if HEADER_PACKAGE in _r and _r[HEADER_PACKAGE] != UNSET:
                    _pkg_label = _r[HEADER_PACKAGE]
                _path_gt_dir = win_drive / _sub_dir / _pkg_label / 'page'
                _path_gt_dir.mkdir(exist_ok=True, parents=True)
                _path_gt = _path_gt_dir / (_d + '.xml')
                open(_path_gt, mode='w').write(_DUMMY_OCR_)
                _generated_paths.append(_path_gt)
                _path_img_dir = win_drive / _sub_dir / _pkg_label
                _path_img_dir.mkdir(exist_ok=True)
                _path_img = _path_img_dir / (_d + '.jpg')
                open(_path_img, mode='wb').write(_DUMMY_JPG_)
                _generated_paths.append(_path_img)
    return _generated_paths


def _upsert_value(the_map, the_key, the_value):
    if the_key in the_map:
        the_map[the_key].append(the_value)
    else:
        the_map[the_key] = [the_value]


def _generate_eval_struct_from(path_data_file, path_ocr_data_root):
    """Util func to generate according filesystem structures
    that fit for the file identifiers from given data_file
    only for entry which are at least kind-a REVIEWED
    """

    _gt_dict = _read_data(path_data_file)
    _generated_paths = {}
    for _d, _v in _gt_dict.items():
        if _v[HEADER_REWV] != UNSET:
            _the_dir = _d.split('_')[1]
            _path_gt_dir = path_ocr_data_root / 'groundtruth' / 'odem' / _the_dir
            _path_gt_dir.mkdir(exist_ok=True)
            _path_gt = _path_gt_dir / (_d + '.gt.xml')
            open(_path_gt, mode='w').write(_DUMMY_OCR_)
            _upsert_value(_generated_paths, _the_dir, _path_gt)
            _path_img_dir = path_ocr_data_root / 'media' / 'jpg' / 'odem' / _the_dir
            _path_img_dir.mkdir(exist_ok=True)
            _path_img = _path_img_dir / (_d + '.jpg')
            open(_path_img, mode='wb').write(_DUMMY_JPG_)
            _upsert_value(_generated_paths, _the_dir, _path_img)
    return _generated_paths


@pytest.fixture(name='create_struct')
def _fixture_create_struct(tmp_path):
    _fixture_root = tmp_path / 'create_gt_fixture'
    _fixture_root.mkdir()
    _odem_gt_root, _ocr_data_root = _generate_fs_structs(_fixture_root)
    return (_fixture_root, _odem_gt_root, _ocr_data_root)


@pytest.fixture(name='gt_sample_inspect')
def _fixture_gt_inspect(create_struct):
    """Generate Fixture for introspection
    Each record's state is expected to fit
    existing files"""

    _fixture_res = create_struct[0] / 'resources'
    _odem_csv_file = _fixture_res / _LABEL_SAMPLE_INSPECT_
    if not os.path.exists(_odem_csv_file):
        _fixture_res.mkdir()
        shutil.copyfile(_PATH_RES_SAMPLE_, _odem_csv_file)
        _generate_win_network_struct_from(_odem_csv_file, create_struct[1])
        _generate_eval_struct_from(_odem_csv_file, create_struct[2])
    return (_odem_csv_file, create_struct[1], create_struct[2])


@pytest.fixture(name='gt_sample_advance')
def _fixture_gt_advance(create_struct):
    """Generate Fixture that represents state
    just before record stages advance, i.e.
    there might already exist resources in
    state 'DONE' directory, but in the csv
    data they lack this information

    Therefore, some resources need to be
    copied on to represent a physical state
    that is *before* what's written in CSV

    * package '2021-12-06-4185-01' to 'STRUCTURED'
    * package '2022-01-03-4185-02' to 'REVIEWED'
    * package '2022-01-11-4185-03' to 'DONE'
    * package '2022-01-25-4185-04' to 'TO_DO'
    """

    _fixture_dir_res = create_struct[0] / 'resources'
    _odem_csv_file = _fixture_dir_res / _LABEL_SAMPLE_
    if not os.path.exists(_odem_csv_file):
        _fixture_dir_res.mkdir()
        shutil.copyfile(_PATH_RES_SAMPLE_, _odem_csv_file)
        _net_path_map = _generate_win_network_struct_from(_odem_csv_file, create_struct[1])
        _copy_ressources_to_next_stage_even_if_exist(_net_path_map)
        _data_path_map = _generate_eval_struct_from(_odem_csv_file, create_struct[2])
    return (_odem_csv_file, create_struct[1], create_struct[2])


def _next_stage(this_stage):
    _i = GT_DATA_FIELDS.index(this_stage)
    _n = _i + 1
    if _n <= len(GT_DATA_FIELDS):
        return GT_DATA_FIELDS[_n]


def _copy_ressources_to_next_stage_even_if_exist(paths_res):
    for _p in paths_res:
        _current = _stage_from_path(Path(_p))
        _next = _next_stage(_current)
        _n = Path(str(_p).replace(HEADER_DIR_MAP[_current], HEADER_DIR_MAP[_next]))
        if not _n.parent.exists():
            _n.parent.mkdir(parents=True)
        shutil.copyfile(_p, _n)


def test_create_next_groundtruth_batch_missing_src(gt_sample_advance):
    """When using setup but missing proper argument
    for last argument 'dry_run' raise TypeError
    """
    # arrange
    gt_file, _odem_root, _ = gt_sample_advance

    # act
    with pytest.raises(TypeError) as sys_ex:
        # sonarlint
        create_next_groundtruth_batch(gt_file, 2, _odem_root)

    # assert
    assert 'TypeError' == sys_ex.typename
    assert 'create_next_groundtruth_batch() missing 1 required positional argument' in sys_ex.value.args[0]


def test_create_next_groundtruth_batch_invalid_src(gt_sample_advance):
    """When using setup with invalid OCR-Data root
    directory halt generation and yield SystemExit(1)
    """

    # arrange
    gt_file, _, _ = gt_sample_advance

    # act
    with pytest.raises(SystemExit) as sys_ex:
        create_next_groundtruth_batch(gt_file, 2, '/foo', False)

    # assert
    assert sys_ex.typename == 'SystemExit'
    assert sys_ex.value.args[0] == 1


def test_create_next_groundtruth_batch_empty_src(gt_sample_advance):
    """When using setup with invalid OCR netdrive
    directory halt generation and yield SystemExit(1)
    """

    # arrange
    gt_file, _, _ = gt_sample_advance

    # act
    with pytest.raises(SystemExit) as sys_ex:
        create_next_groundtruth_batch(gt_file, 2, '/foo/bar', True)

    # assert
    assert sys_ex.typename == 'SystemExit'
    assert sys_ex.value.args[0] == 1


def test_create_small_todo_batch_from_sample(gt_sample_inspect):
    """Reasonable example with slim arguments:
    path_to_data_file=csv_file, sample_size=2,
    odem_project_root_path=root_dir, dry_run=False

    Marks records
    * urn+nbn+de+gbv+3+1-651813-p0077-2_ger
    * urn+nbn+de+gbv+3+1-651813-p0077-2_ger
    as to_do (only these are open) and 
    Create new sub folder
    * <today>-4185-02

    Must use gt_dample_inspect Fixture, because no
    differences between what's known at CSV and
    physical files must exist
    """

    # arrange
    _r_one = 'urn+nbn+de+gbv+3+1-651813-p0077-2_ger'
    _r_two = 'urn+nbn+de+gbv+3+1-337225-p0077-4_ger'
    csv_file, win_share, _ = gt_sample_inspect
    win_share = str(win_share).replace(GT_DOMAIN,'')
    _before = _read_data(csv_file)
    assert _before[_r_one][HEADER_TODO] == UNSET
    assert _before[_r_two][HEADER_TODO] == UNSET

    # act:
    _outcome = create_next_groundtruth_batch(csv_file, 2, win_share, False)

    # assert
    assert len(_outcome) == 2
    _created_dir = _outcome[0]
    # we know this for sure from testdata
    assert _created_dir.endswith('4185-04')
    assert _outcome[1] == 2
    _after = _read_data(csv_file)
    assert _after[_r_one][HEADER_TODO].startswith(_TODAY_)
    assert _after[_r_one][HEADER_PACKAGE] == '2022-01-25-4185-04'
    assert _after[_r_two][HEADER_TODO].startswith(_TODAY_)
    assert _after[_r_two][HEADER_PACKAGE] == '2022-01-25-4185-04'
    # ensure image exists at expected location (2nd directory)
    assert os.path.exists(f'{_created_dir}/{_r_one}.jpg')


@pytest.fixture(name='gt_full')
def _fixture_gt_full(create_struct):
    """generate data holding small csv-sample"""
    _fixture_res = create_struct[0] / 'resources'
    _odem_csv_file = _fixture_res / _LABEL_PROD_FILE_
    if not os.path.exists(_odem_csv_file):
        _fixture_res.mkdir()
        shutil.copyfile(_PATH_PROD_, _odem_csv_file)
        _generate_win_network_struct_from(_odem_csv_file, create_struct[1])
        _generate_eval_struct_from(_odem_csv_file, create_struct[2])
    return (_odem_csv_file, create_struct[1], create_struct[2])


@pytest.mark.skip("Nope, creates x1000 test artefacts")
def test_create_common_todo_batch(gt_full):
    """Reasonable data with slim arguments:
    path_to_data_file=csv_file, sample_size=2,
    odem_project_root_path=root_dir, dry_run=False

    Create new sub folder
    * <today>-4185-02
    """

    # arrange
    csv_file, win_share, _ = gt_full

    # act:
    _outcome = create_next_groundtruth_batch(csv_file, 40, win_share, False)

    # assert
    assert len(_outcome) == 2
    _created_dir = _outcome[0]
    assert _created_dir.endswith('4185-02')
    assert _outcome[1] == 40
    # ensure 40 images exists at expected location (2nd directory)
    _new_imgs = [e for e in os.listdir(_created_dir) if e.endswith('.jpg')]
    assert len(_new_imgs) == 40


@pytest.mark.skip("Nope, this changes with the csv data")
def test_inspect_common_data(gt_full):
    """Inspect reasonable, but synthetic data vs. entries
    from *real* csv-gt-data file.

    Because all files are created synthetically on-th-fly,
    they *all* considered newer than what's present in
    the csv file and therefore all marked as UPDATE 
    """

    # arrange
    csv_file, win_share, ocr_data = gt_full

    # act
    _results = inspect(csv_file, project_root_path=win_share, odem_data_share=ocr_data)

    # assert 1603 results because 7 additional hebrew records
    assert len(_results) == 1602


def test_inspect_sample_data(gt_sample_inspect):
    """Inspect small sample data set with
    differences between CSV Data state and
    filesystem state.

    The first two records are advanced 
    already on the project space filesystem,
    but the CSV data doesn't know =>
    must recognize this out-of-sync state!
    """

    # arrange
    csv_file, win_share, ocr_data = gt_sample_inspect
    path_data_groundtruth = ocr_data / 'groundtruth' / 'odem'
    path_data_media = ocr_data / 'media' / 'jpg' / 'odem'

    # act
    _susp = inspect(csv_file, win_share, path_data_groundtruth, path_data_media, [])

    # assert
    assert len(_susp) == 2
    assert URN_NO_04 in _susp
    assert URN_NO_03 in _susp


def test_inspect_sample_data_image_file_missing(gt_sample_inspect):
    """Inspect small sample data set with
    differences between CSV Data state and
    filesystem state.

    We don't care for missing images => not suspect anymore
    """

    # arrange
    csv_file, win_share, ocr_data = gt_sample_inspect
    path_data_groundtruth = ocr_data / 'groundtruth' / 'odem'
    path_data_media = ocr_data / 'media' / 'jpg' / 'odem'
    _package_rev = win_share / DIR_REVIEWED / '2021-12-06-4185-01'
    os.unlink(_package_rev / f'{URN_NO_03}.jpg')

    # act
    _susp = inspect(csv_file, win_share, path_data_groundtruth, path_data_media, [HEADER_STRC])

    # assert
    assert len(_susp) == 0


def test_inspect_sample_data_ocr_file_missing(gt_sample_inspect):
    """Inspect small sample data set with
    differences between CSV Data state and
    filesystem state.

    OCR file for groundtruth from reviewed directory
    missing in filesystem, but record marked 'DONE' in CSV
    => 
    must recognize out-of-sync state!
    """

    # arrange
    csv_file, win_share, ocr_data = gt_sample_inspect
    path_data_groundtruth = ocr_data / 'groundtruth' / 'odem'
    path_data_media = ocr_data / 'media' / 'jpg' / 'odem'
    _package_rev = win_share / DIR_DONE / '2022-01-03-4185-02'
    os.unlink(_package_rev / 'page' / f'{URN_NO_01}.xml')

    # act
    _susp = inspect(csv_file, win_share, path_data_groundtruth, path_data_media, [HEADER_STRC])

    # assert
    assert len(_susp) == 1
    assert URN_NO_01 in _susp
    assert _susp[URN_NO_01] == [HEADER_DONE]


def test_inspect_sample_data_files_and_dir_missing(gt_sample_inspect):
    """Inspect small sample data set with
    differences between CSV Data state and
    filesystem state.

    Complete directory is deleted on disc,
    files are deleted from /data/ocr, but
    it's records are marked 'DONE' in CSV
    => 
    must recognize out-of-sync state!
    """

    # arrange
    csv_file, win_share, ocr_data = gt_sample_inspect
    path_data_groundtruth = ocr_data / 'groundtruth' / 'odem'
    path_data_media = ocr_data / 'media' / 'jpg' / 'odem'
    _package_rev = win_share / DIR_REVIEWED / '2021-12-06-4185-01'
    shutil.rmtree(_package_rev)
    os.unlink(path_data_groundtruth / 'lat' / f'{URN_NO_03}.gt.xml')
    os.unlink(path_data_media / 'lat' / f'{URN_NO_03}.jpg')
    os.unlink(path_data_groundtruth / 'lat+ger' / f'{URN_NO_04}.gt.xml')
    os.unlink(path_data_media / 'lat+ger' / f'{URN_NO_04}.jpg')

    # act
    _susp = inspect(csv_file, win_share, path_data_groundtruth, path_data_media, [HEADER_STRC])

    # assert
    assert len(_susp) == 2
    assert URN_NO_03 in _susp
    assert _susp[URN_NO_03] == [HEADER_REWV]
    assert URN_NO_04 in _susp
    assert _susp[URN_NO_04] == [HEADER_REWV]


def test_inspect_sample_data_check_datetime(gt_sample_inspect):
    """Inspect small sample data set with
    differences between CSV Data state and
    filesystem state.

    Since all test resource files are
    created on-the-fly, they *always*
    differ from what's annotated at
    record level, so *all* 6 records
    are suspect this way!
    => 
    must recognize out-of-sync state!
    """

    # arrange
    csv_file, win_share, ocr_data = gt_sample_inspect
    path_data_groundtruth = ocr_data / 'groundtruth' / 'odem'
    path_data_media = ocr_data / 'media' / 'jpg' / 'odem'

    # act
    _susp = inspect(csv_file, win_share, 
                    path_data_groundtruth, 
                    path_data_media, 
                    ignore_stages=[HEADER_STRC], 
                    check_dates=True)

    # assert
    assert len(_susp) == 6
    assert URN_NO_01 in _susp
    assert URN_NO_02 in _susp
    assert URN_NO_03 in _susp
    assert URN_NO_04 in _susp


def test_advance_next_stage_dry(gt_sample_advance):
    """Behavior: simulate dry data advance
    for package '2022-01-03-4185-02' to 'REVIEWED
    with physical files already located there"""

    # arrange
    csv_file, project_root, data_root = gt_sample_advance
    start_dir = os.path.join(project_root, DIR_REVIEWED, '2022-01-03-4185-02')
    prev_data = _read_data(csv_file)
    assert prev_data[URN_NO_01][HEADER_REWV] == UNSET
    assert prev_data[URN_NO_02][HEADER_REWV] == UNSET

    # act
    result = advance(csv_file, start_dir, data_root)
    curr_data = _read_data(csv_file)

    # assert
    assert result == (HEADER_REWV, 2, 2)
    # ensure no changes in csv data
    assert curr_data[URN_NO_01][HEADER_REWV] == UNSET
    assert curr_data[URN_NO_02][HEADER_REWV] == UNSET


def test_advance_next_stage_done_hot(gt_sample_advance):
    """Behavior: simulate data advance

    check that both
    * urn+nbn+de+gbv+3+1-170911-p0077-5_ger
    * urn+nbn+de+gbv+3+1-709609-p0289-4_ger

    have state 'DONE' with today's datetime
    """

    # arrange
    urn_01 = 'urn+nbn+de+gbv+3+1-170911-p0077-5_ger'
    urn_02 = 'urn+nbn+de+gbv+3+1-709609-p0289-4_ger'
    csv_file, project_root, data_root = gt_sample_advance
    prev_data = _read_data(csv_file)
    assert prev_data[urn_01][HEADER_DONE] == UNSET
    assert prev_data[urn_02][HEADER_DONE] == UNSET
    start_dir = os.path.join(project_root, DIR_DONE, '2022-01-11-4185-03')

    # act
    result = advance(csv_file, start_dir, data_root, dry_run=False)
    curr_data = _read_data(csv_file)

    # assert
    assert result == (HEADER_DONE, 2, 2)
    assert curr_data[urn_01][HEADER_DONE].startswith(_TODAY_)
    assert curr_data[urn_02][HEADER_DONE].startswith(_TODAY_)


def test_advance_next_stage_done_hot(gt_sample_advance):
    """Behavior: simulate data advance
    with mixed record states, where record
    * urn+nbn+de+gbv+3+1-170911-p0077-5_ger
    has already been set!
    """

    # arrange
    urn_01 = 'urn+nbn+de+gbv+3+1-170911-p0077-5_ger'
    urn_02 = 'urn+nbn+de+gbv+3+1-709609-p0289-4_ger'
    csv_file, project_root, data_root = gt_sample_advance
    prev_data = _read_data(csv_file)
    assert prev_data[urn_01][HEADER_DONE] == UNSET
    assert prev_data[urn_02][HEADER_DONE] == UNSET
    start_dir = os.path.join(project_root, DIR_DONE, '2022-01-11-4185-03')

    # act
    prev_data[urn_01][HEADER_DONE] = _TODAY_
    _write_data(csv_file, prev_data, suffix='.csv')
    result = advance(csv_file, start_dir, data_root, dry_run=False, force_overwrite=True)
    curr_data = _read_data(csv_file)

    # assert
    assert result == (HEADER_DONE, 2, 2)
    assert curr_data[urn_01][HEADER_DONE].startswith(_TODAY_)
    assert curr_data[urn_02][HEADER_DONE].startswith(_TODAY_)


def test_advance_to_reviewed_hot(gt_sample_advance):
    """Behavior: simulate data advance when
    files not present at target destination

    check that both
    * urn+nbn+de+gbv+3+1-138408-p0042-0_lat
    * urn+nbn+de+gbv+3+1-182075-p0034-1_ger

    have state 'REVIEWED' with today's datetime
    """

    # arrange
    csv_file, project_root, data_root = gt_sample_advance
    prev_data = _read_data(csv_file)
    assert prev_data[URN_NO_01][HEADER_REWV] == UNSET
    assert prev_data[URN_NO_02][HEADER_REWV] == UNSET
    csv_file, project_root, data_root = gt_sample_advance
    start_dir = os.path.join(project_root, DIR_REVIEWED, '2022-01-03-4185-02')

    # act
    result = advance(csv_file, start_dir, data_root, dry_run=False)
    curr_data = _read_data(csv_file)

    # assert
    assert result == ('REVIEWED', 2, 2)
    assert curr_data[URN_NO_01][HEADER_REWV].startswith(_TODAY_)
    assert curr_data[URN_NO_02][HEADER_REWV].startswith(_TODAY_)


def test_advance_to_reviewed_fails_due_data_exist(gt_sample_advance):
    """Behavior: simulate data advance when
    * state already marked
    or
    * files already present at target destination

    check that both
    * urn+nbn+de+gbv+3+1-142331-p0182-2_lat+ger
    * urn+nbn+de+gbv+3+1-137009-p0597-2_lat

    have state 'REVIEWED' with today's datetime

    => Fails *not* in dry_mode
    """

    # arrange
    csv_file, project_root, data_root = gt_sample_advance
    start_dir = os.path.join(project_root, DIR_REVIEWED, '2021-12-06-4185-01')

    # act
    with pytest.raises(RuntimeError) as run_err:
        advance(csv_file, start_dir, data_root, False)

    # assert
    assert '[2021-12-06-4185-01][ERROR] already marked as REVIEWED' in run_err.value.args[0]


@pytest.mark.parametrize(['file_path', 'is_img'], [
    ('to_do_AFa/2023-03-01-4185-01/urn+nbn+de+gbv+3+1-138408-p0042-0_lat.jpg', True),
    ('to_do_AFa/2023-03-01-4185-01/thumbs/urn+nbn+de+gbv+3+1-138408-p0042-0_lat.jpg', False),
    ('odem_groundtruth_4185/MAX/urn+nbn+de+gbv+3+1-138408-p0042-0_lat.jpg', False),
    ('to_do_AFa/2023-03-01-4185-01/page/urn+nbn+de+gbv+3+1-138408-p0042-0_lat.xml', False),
])
def test_filter_img(file_path, is_img):

    # act
    assert _filter_project_img(file_path) == is_img
