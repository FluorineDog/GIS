# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# pylint: disable=too-many-lines
# pylint: disable=too-many-public-methods, unused-argument, redefined-builtin,protected-access

from warnings import warn

import numpy as np
import pandas as pd

# pylint: disable=too-many-branches,too-many-statements
def sjoin(
        left_df, right_df, lcol, rcol, how="inner", op="intersects", lsuffix="left", rsuffix="right"
):
    """
    Spatial join of two GeoDataFrames.

    Parameters
    ----------
    left_df, right_df : GeoDataFrames
    rcol, lcol : str
        Specify geometry columns of left_df, right_df to be joined.
    how : string, default 'inner'
        The type of join:

        * 'left': use keys from left_df; retain only left_df geometry column
        * 'right': use keys from right_df; retain only right_df geometry column
        * 'inner': use intersection of keys from both dfs; retain only
          left_df geometry column
    op : string, default 'intersects'
        Binary predicate, one of {'intersects', 'contains', 'within'}.
    lsuffix : string, default 'left'
        Suffix to apply to overlapping column names (left GeoDataFrame).
    rsuffix : string, default 'right'
        Suffix to apply to overlapping column names (right GeoDataFrame).

    Returns
    -------
    GeoDataFrame
        An arctern.GeoDataFrame object.

    Examples
    ---------
    >>> import arctern
    >>> from arctern import GeoDataFrame
    >>> import numpy as np
    >>> data1 = {
    >>>      "A": range(5),
    >>>      "B": np.arange(5.0),
    >>>      "geometry": ["LINESTRING (0 0, 2 2)", "LINESTRING (1 0,1 3)", "LINESTRING (9 3, 3 5)", "LINESTRING (4 5, 6 7)",
    >>>                   "LINESTRING (7 7, 9 9)", ],
    >>> }
    >>> gdf1 = GeoDataFrame(data1, geometries=["geometry"], crs=["epsg:4326"])
    >>> data2 = {
    >>>       "C": range(5),
    >>>       "location": ["LINESTRING (1 0, 3 4)", "LINESTRING (0 0, 6 7)", "LINESTRING (5 3, 8 7)", "LINESTRING (9 8, 4 7)",
    >>>                    "LINESTRING (3 3, 6 9)", ]
    >>> }
    >>> gdf2 = GeoDataFrame(data2, geometries=["location"], crs=["epsg:4326"])
    >>> arctern.sjoin(left_df=gdf1, right_df=gdf2, lcol="geometry", rcol="location")
    """
    from arctern import GeoDataFrame, GeoSeries
    if not isinstance(left_df, GeoDataFrame):
        raise ValueError(
            "'left_df' should be GeoDataFrame, got {}".format(type(left_df))
        )

    if not isinstance(right_df, GeoDataFrame):
        raise ValueError(
            "'right_df' should be GeoDataFrame, got {}".format(type(right_df))
        )

    allowed_hows = ["left", "right", "inner"]
    if how not in allowed_hows:
        raise ValueError(
            '`how` was "%s" but is expected to be in %s' % (how, allowed_hows)
        )

    allowed_ops = ["contains", "within", "intersects"]
    if op not in allowed_ops:
        raise ValueError(
            '`op` was "%s" but is expected to be in %s' % (op, allowed_ops)
        )

    if left_df[lcol].crs != right_df[rcol].crs:
        warn(
            (
                "CRS of frames being joined does not match!"
                "(%s != %s)" % (left_df.crs, right_df.crs)
            )
        )

    index_left = "index_%s" % lsuffix
    index_right = "index_%s" % rsuffix

    if any(left_df.columns.isin([index_left, index_right])) or any(
        right_df.columns.isin([index_left, index_right])
    ):
        raise ValueError(
            "'{0}' and '{1}' cannot be names in the frames being"
            " joined".format(index_left, index_right)
        )

    if right_df[rcol]._sindex_generated or (
            not left_df[lcol]._sindex_generated and right_df.shape[0] > left_df.shape[0]
    ):
        tree_idx = right_df[rcol].sindex
        tree_idx_right = True
    else:
        tree_idx = left_df[lcol].sindex
        tree_idx_right = False

    # the rtree spatial index only allows limited (numeric) index types, but an
    # index in geopandas may be any arbitrary dtype. so reset both indices now
    # and store references to the original indices, to be reaffixed later.
    # GH 352
    left_df = left_df.copy(deep=True)
    try:
        left_index_name = left_df.index.name
        left_df.index = left_df.index.rename(index_left)
    except TypeError:
        index_left = [
            "index_%s" % lsuffix + str(l) for l, ix in enumerate(left_df.index.names)
        ]
        left_index_name = left_df.index.names
        left_df.index = left_df.index.rename(index_left)
    left_df = left_df.reset_index()

    right_df = right_df.copy(deep=True)
    try:
        right_index_name = right_df.index.name
        right_df.index = right_df.index.rename(index_right)
    except TypeError:
        index_right = [
            "index_%s" % rsuffix + str(l) for l, ix in enumerate(right_df.index.names)
        ]
        right_index_name = right_df.index.names
        right_df.index = right_df.index.rename(index_right)
    right_df = right_df.reset_index()

    if op == "within":
        left_df, right_df = right_df, left_df
        tree_idx_right = not tree_idx_right

    r_idx = np.empty((0, 0))
    l_idx = np.empty((0, 0))

    # get rtree spatial index
    if tree_idx_right:
        idxmatch = left_df[lcol].apply(
            lambda x: list(tree_idx.query(GeoSeries(x))) if not x == () else []
        )
        idxmatch = idxmatch[idxmatch.apply(len) > 0]
        # indexes of overlapping boundaries
        if idxmatch.shape[0] > 0:
            r_idx = np.concatenate(idxmatch.values)
            l_idx = np.concatenate([[i] * len(v) for i, v in idxmatch.iteritems()])
    else:
        # tree_idx_df == 'left'
        idxmatch = right_df[rcol].apply(
            lambda x: list(tree_idx.query(GeoSeries(x))) if not x == () else []
        )
        idxmatch = idxmatch[idxmatch.apply(len) > 0]
        if idxmatch.shape[0] > 0:
            # indexes of overlapping boundaries
            l_idx = np.concatenate(idxmatch.values)
            r_idx = np.concatenate([[i] * len(v) for i, v in idxmatch.iteritems()])

    if len(r_idx) > 0 and len(l_idx) > 0:
        # Vectorize predicate operations
        def find_intersects(a1, a2):
            return GeoSeries(a1).intersects(GeoSeries(a2))[0]

        def find_contains(a1, a2):
            return GeoSeries(a1).contains(GeoSeries(a2))[0]

        def find_within(a1, a2):
            return GeoSeries(a1).within(GeoSeries(a2))[0]

        predicate_d = {
            "intersects": find_intersects,
            "contains": find_contains,
            "within": find_within,
        }

        check_predicates = np.vectorize(predicate_d[op])

        result = pd.DataFrame(
            np.column_stack(
                [
                    l_idx,
                    r_idx,
                    check_predicates(
                        left_df[lcol][l_idx],
                        right_df[rcol][r_idx],
                    ),
                ]
            )
        )

        result.columns = ["_key_left", "_key_right", "match_bool"]
        result = pd.DataFrame(result[result["match_bool"] == 1]).drop(
            "match_bool", axis=1
        )

    else:
        # when output from the join has no overlapping geometries
        result = pd.DataFrame(columns=["_key_left", "_key_right"], dtype=float)

    if how == "inner":
        result = result.set_index("_key_left")
        joined = (
            left_df.merge(result, left_index=True, right_index=True)
            .merge(
                right_df.drop(rcol, axis=1),
                left_on="_key_right",
                right_index=True,
                suffixes=("_%s" % lsuffix, "_%s" % rsuffix),
            )
            .set_index(index_left)
            .drop(["_key_right"], axis=1)
        )
        if isinstance(index_left, list):
            joined.index.names = left_index_name
        else:
            joined.index.name = left_index_name

    elif how == "left":
        result = result.set_index("_key_left")
        joined = (
            left_df.merge(result, left_index=True, right_index=True, how="left")
            .merge(
                right_df.drop(rcol, axis=1),
                how="left",
                left_on="_key_right",
                right_index=True,
                suffixes=("_%s" % lsuffix, "_%s" % rsuffix),
            )
            .set_index(index_left)
            .drop(["_key_right"], axis=1)
        )
        if isinstance(index_left, list):
            joined.index.names = left_index_name
        else:
            joined.index.name = left_index_name

    else:  # how == 'right':
        joined = (
            left_df.drop(lcol, axis=1)
            .merge(
                result.merge(
                    right_df, left_on="_key_right", right_index=True, how="right"
                ),
                left_index=True,
                right_on="_key_left",
                how="right",
            )
            .set_index(index_right)
            .drop(["_key_left", "_key_right"], axis=1)
        )
        if isinstance(index_right, list):
            joined.index.names = right_index_name
        else:
            joined.index.name = right_index_name

    return joined
