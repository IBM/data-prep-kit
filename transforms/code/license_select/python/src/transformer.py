# (C) Copyright IBM Corp. 2024.
# Licensed under the Apache License, Version 2.0 (the “License”);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an “AS IS” BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

from collections.abc import Callable, Iterable
from typing import TypeVar

import pyarrow
import pyarrow.compute as pc
from pyarrow import ListScalar, Table


T = TypeVar("T")
R = TypeVar("R")

Mapping = Callable[[T], R]


def exact_matcher(matches: Iterable[T]) -> Mapping[T, bool]:
    """
    Returns a Mapping that generates a boolean indicating whether an input exactly matches any of a set of values.
    """
    unique_matches = set(matches)
    return lambda name: name in unique_matches


class AllowLicenseStatusTransformer:
    """
    Transforms input tables by adding a boolean `license_status` column indicating whether the license is approved.
    """

    def __init__(
        self,
        license_column: str = None,
        allow_no_license: bool = False,
        licenses: list[str] = None,
    ):
        self._license_column = license_column or "license"

        if not licenses:
            raise TypeError("No approved licenses found.")
        licenses = [_lower_case(license) for license in licenses]
        if allow_no_license:
            licenses.append(None)

        self._string_transformer = LicenseStringTransformer(licenses)
        self._list_transformer = LicenseListTransformer(licenses)

    def transform(self, data: Table) -> Table:
        license_type = data.schema.field(self._license_column).type

        data = _rename_column(data, self._license_column, "license")

        if pyarrow.types.is_string(license_type) or pyarrow.types.is_null(license_type):
            return self._string_transformer.transform(data)
        if license_type.equals(pyarrow.list_(pyarrow.string())):
            return self._list_transformer.transform(data)

        raise TypeError(f"Invalid {self._license_column} column type: {license_type}")


def _lower_case(value: str | None) -> str | None:
    if value == None:
        return None
    return str.casefold(value)


class DenyLicenseStatusTransformer:
    def __init__(
        self,
        license_column: str = None,
        allow_no_license: bool = False,
        licenses: list[str] = None,
    ):
        self._transformer = AllowLicenseStatusTransformer(
            license_column=license_column,
            allow_no_license=(not allow_no_license),
            licenses=licenses,
        )

    def transform(self, data: Table) -> Table:
        result = self._transformer.transform(data)
        license_status = pc.invert(result.column("license_status"))
        return _update_column(result, "license_status", license_status)


class LicenseStringTransformer:
    """
    Transforms input tables with a string type license column
    """

    def __init__(self, allowed_licenses: Iterable[str]):
        self._allowed_licenses = pyarrow.array(allowed_licenses)

    def transform(self, data: Table) -> Table:
        license_type = data.schema.field("license").type

        if pyarrow.types.is_null(license_type):
            orig_licenses = data.column("license")
            licenses = pc.cast(orig_licenses, pyarrow.string())
        else:
            licenses = pc.utf8_lower(data.column("license"))

        license_status = pc.is_in(licenses, value_set=self._allowed_licenses, skip_nulls=False)
        return data.append_column("license_status", license_status)


class LicenseListTransformer:
    """
    Transforms input tables with a list type license column
    """

    def __init__(self, allowed_licenses: Iterable[str]):
        self._matcher = exact_matcher(allowed_licenses)

    def transform(self, data: Table) -> Table:
        statuses = map(self._license_status, data.column("license"))
        status_column = pyarrow.array(statuses, type=pyarrow.bool_())
        return data.append_column("license_status", status_column)

    def _license_status(self, licenses: ListScalar) -> bool:
        licenses = licenses.as_py()
        if len(licenses) == 0:
            licenses.append(None)

        for license in map(_lower_case, licenses):
            if not self._matcher(license):
                return False

        return len(licenses) > 0


def _rename_column(data: Table, from_column: str, to_column: str) -> Table:
    if from_column == to_column:
        return data

    new_names = [to_column if name == from_column else name for name in data.column_names]
    return data.rename_columns(new_names)


def _update_column(data: Table, column_name: str, column: pyarrow.Array) -> Table:
    column_index = data.column_names.index(column_name)
    return data.set_column(column_index, column_name, column)
