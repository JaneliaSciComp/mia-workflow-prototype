# Gaps in the dataset catalog, and what the pipeline infers to cover them

2026-10-28. Catalog `AI-HHMI/mia-agentic-search` at `a6f3d72773b3581dfdc6f798911f9887483c2455` (the ref the
keeper synced on 2026-10-27): 2,053 records, 1,238 2D, 134 2D+t, 601 3D, 80 3D+t. Counts come from running
`mia_buffers.specmap.facts` over every record, posting a permissive spec (all four dimensionalities, every licence
the catalog uses, annotations optional, patch 256, levels 1,2,4) to the live `POST /plan`, and reading the 218
mia-data issues and 2,121 merged catalog PRs. This extends the table in the training-buffers spec §9.8, which was
taken at `ab6a013d` (1,940 records).

## 1. The situation in one table

The planner needs a fixed list of facts per record. Here is where each one comes from, how often it is missing,
what the pipeline does about it today, whether something could be inferred instead, and how far that inference
can be trusted. Confidence is graded by how often the inference would be right across the catalog: **high** means
the direct evidence is in the record and the rule holds for nearly every case, **medium** means a plausible
default with a known spread, **low** means a guess that is as likely wrong as right.

| fact | catalog field | missing (of 2,053) | effect today | inference available | confidence |
|---|---|---|---|---|---|
| dimensionality | `imaging.dimensionality` | 0 | selects the patch block | none needed; 9 2D records carry a z or t axis in their arrays (see §3.11) | high |
| raw array shape | `technical.arrays[]` role raw, `axes` + `shape` | 269 (13%) | "no array shape": out of the pool | 116 of these have a shape but null axes; shape length settles 31 as yx and 62 as zyx; 80 3D stacks of 2D files have z in `layout.n_files` | high for 2-long shapes and for 3-long shapes on a 3D record; medium when the file count stands in for z |
| largest item, when shapes vary | `shape_varies` | 637 raw arrays true, 564 null | the representative item is taken as the extent; 181 footprint rejections rest on it | the listing tree sometimes names the largest file; otherwise only reading headers settles it | low from the record alone |
| voxel size y, x | `imaging.voxel_size_nm` | 1,237 (60%) | filled from the modality median table (`assume.py`), row marked assumed | 119 records state a pixel size in prose ("0.87 nm/pixel"); 377 give only a magnification | high where the prose states it; the table is medium for EM (85 to 92% of documented values within a factor of two of the median) and low for most LM (18 to 53%) |
| voxel size z | `imaging.voxel_size_nm.z` | 274 of 681 3D records (40%) | median z step per modality | xz cross-section check against the pixels, as the spec's ingestion agent would do | medium for FIB-SEM and cryo-ET, low elsewhere |
| channel count | `imaging.channels`, `axes` c or s | 480 `channels` null; 231 disagree with the array | the array's count wins; `channels: 1` on a 1,200 by 1,600 by 3 PNG is read as RGB | the array is the better source; the disagreement itself is a data-quality signal | high (array), low (`imaging.channels`) |
| channel meaning, gray and RGB recipes | not in the schema | all multi-channel records | recipe from count and modality (`channel_recipes`) | `labels_stains` and `sample` text name the stains; a one-dominant-hue RGB PNG is a gray image stored as RGB | medium |
| intensity range | `value_range`, `normalization` | 791 raw arrays without a range, 2,231 `normalization: unknown` | percentile normalisation per volume from the stage job's sample | none needed: measured at staging | high (measured) |
| organism, exactly one | `imaging.organism` | 402 empty, 262 more than one | no mia-data folder: 733 records excluded before the plan, 728 of them for the organism | empty mostly means a non-biological or mixed sample (alloys, rock, phytoplankton, cell lines named in `sample`); multi-organism sets split per file set in `layout` | medium: a species can be read from `sample` for cell lines, but materials science records have no organism to find |
| modality, one family | `imaging.modality` | 241 `other`, 271 more than one | `other` with no organism excludes 138; 75 multi-modality sets have no single family | `sample` and `description` name the instrument (SEM of shale, STEM of catalysts, IFCB plankton) | medium; most `other` records are SEM or STEM of materials and would need a new family, not a better guess |
| licence | `license.spdx` | 3 unknown, 89 `custom` | gated by `allow_unknown`; 10 real SPDX ids are not on the page's or the example spec's list | none; the list is the gap | high |
| direct download link | `data.download_url`, `technical.sample.urls` | 291 null, 467 the landing page, 692 a page or API; 209 without sample URLs | mia-data must find the files itself; 46 of the 49 datasets it handed to a human failed on host policy or redirects, not on the record | the repository APIs (Zenodo, figshare, EBI) resolve a landing page to files deterministically | high for Zenodo, figshare, BioImage Archive, EMPIAR (1,372 records); low for lab websites and `other` |
| size | `data.size_bytes`, `technical.size_source` | 54 null; 143 estimated, 57 unknown; 271 listings incomplete | quota planning and the keeper's hold | file listing where the host has one | high where listed, low for estimates |
| item count | `data.n_items` | 96 null | plan positions and splits for multi-item sets | `layout.n_files` | medium: files are not items when several files make one item |
| annotation facts | `annotations.*`, label arrays | 356 "present" with no label array; 315 source unknown; 105 coverage unknown; 180 label encodings unknown; 263 alignments unknown; 103 unverified | filters on type and source pass; the label is discovered or dropped at binding time | mia-data's binding settles key, encoding and alignment per label and records the reasons | high after preparation, low before |
| identity for dedup | `doi` | 403 (20%) | `dedup.by: [doi]` cannot see duplicates among them | `landing_url`, `accession`, `publications[].doi`, the catalog's own `tools/dedup.py` keys | high |

## 2. How the facts flow, and where inferences live

A record is read once by `mia_buffers.specmap.facts`, which never raises on a missing field: a null becomes
"no arrays", "no voxel size", "no organism", and the planner turns each into a rule string. Two places then
guess. `mia_buffers/assume.py` fills missing voxel axes from a table of catalog medians by modality and derives
the gray and RGB recipes from channel count and modality; the plan row says which axes were assumed.
`mia_data.catalog.dataset_folder` infers the dataset's folder from the first modality's family and the single
organism, and refuses when either is unclear. Everything else is taken literally.

Preparation learns far more than the planner knows. The binding for the prepared B. subtilis set
(`zenodo-5639253-deepbacs-bsubtilis-ftsz`) settles the axes as yx stacked to zyx, names the label keys and
encodings with a reason each, and reads the voxel size from the record because the TIFFs lack one; every crop's
`crop.json` records the measured source shape, dtype and intensity percentiles. None of this returns to the
catalog or to the registry's `records` table, so the same record still plans as "no array shape" (its raw array
has a 1,024 by 1,024 shape and null axes). The prepared pool is therefore smaller than the prepared set: of 7
prepared datasets, 1 is in the pool at patch 256 and levels 1,2,4; 5 fail the footprint and 1 the shape.

## 3. The gaps in detail

### 3.1 Array geometry (269 records, 13%)

| subtype | records | examples |
|---|---|---|
| raw `axes` null, `shape` given | 116 | `zenodo-20174259-mcellseg-hek-huvec`, `zenodo-14232530-frog-synapse-tem-micrographs` |
| raw `shape` null | 93 | `zenodo-17025576-stem-denoising-pairs`, `zenodo-8351731-aimseg-corpus-callosum-tem` |
| 3D record whose raw axes are `yx` (one file per plane) | 72 | `empiar-10791-mouse-liver-fib-sem-organelles`, `edmond-wl9uqn-dm-antennal-lobe-fib-sem` |
| `technical.arrays` empty | 35 | `dryad-qz612jmgv-ant-brain-micro-ct` |
| no `technical` block at all (schema 1.1 records) | 16 | `epfl-hippocampus-mitochondria-em`, `empiar-13420-macrophage-a431-sem` |
| arrays listed, none with role `raw` | 9 | `figshare-12206270-potato-tuber-cells` |
| 3D record, raw axes `yxs`, `tyx` or `tcyx` | 13 | `empiar-12674-mouse-kidney-fib-sem`, `zenodo-8024696-xlfm-zebrafish-2d-3d` |

What the 116 null-axes shapes imply by their length: 31 are 2-long (yx), 62 are 3-long on a 3D record (zyx),
18 are 3-long ending in 3 or 4 (yxs, RGB), 36 are 3-long on a 2D record (c, z or t first: ambiguous), 24 are 4- or
5-long (ambiguous). The schema says axes are "null if the file does not say", so the enricher is following its
rule; the planner could apply the first three inferences itself and mark the row, the way it marks assumed voxel
sizes. For the 72 plane-per-file stacks, `layout.n_files` is set on 80 of the 85 3D records lacking z, and the
file count is the z extent whenever the listing holds one folder of planes; where labels sit in a sibling folder
the count must be halved, which the tree in `layout.tree` shows.

Shape varies on 637 raw arrays and is null on 564 more. The planner checks the footprint against the
representative item; 181 of the 548 footprint rejections rest on such an item, and some of those sets surely
contain larger images. Only a header read of the listed files settles it.

### 3.2 Voxel size (1,237 records without one, 60%)

Missing by modality: brightfield 80%, `other` 84%, TEM 83%, fluorescence-other 71%, DIC 71%, widefield 64%,
super-resolution 61%, light-sheet 59%, histology-WSI 56%, phase-contrast 56%, confocal 54%, spinning-disk 46%,
X-ray 45%, cryo-EM 45%, two-photon 44%, ssSEM 38%, cryo-ET 32%, ssTEM 31%, SBF-SEM 30%, FIB-SEM 17%,
expansion 0%. By dimensionality: 918 of the 1,238 2D records, 201 of 601 3D records plus 20 more with x and y but
no z. The catalog's own `voxel-size-missing` label sits on 1,290 of 2,121 merged PRs.

How good the median table is, from the records that do document a size (the share of documented x values within
a factor of two of the modality median): FIB-SEM 92%, cryo-ET 92%, histology-WSI 81%, SBF-SEM 81%, ssSEM 75%,
super-resolution 71%, ssTEM 70%, cryo-EM 67%, TEM 58%, brightfield 53%, spinning-disk 53%, two-photon 52%, X-ray
49%, confocal 46%, fluorescence-other 42%, widefield 39%, light-sheet 33%, `other` 26%, phase-contrast 18%. For z
steps: FIB-SEM 85%, cryo-ET 92%, SBF-SEM 75%, confocal 49%, light-sheet 39%, X-ray 42%. The table is a fair
stand-in for EM and a coin toss for light microscopy, where the same modality spans 50 nm to 1.3 µm pixels. The
spinning-disk entry (110 nm) sits below the current median (200 nm) and the DIC entry (420 nm) above it (190 nm);
both were medians at `ab6a013d` over fewer records.

What the prose holds, for the 1,237 records without a size: 119 state a pixel size outright ("0.042 nm/pixel",
"5.938 nm/px", "0.220 nm per pixel"), 377 state only a magnification (which gives a size only with the detector's
pixel pitch), 387 mention a length not tied to a pixel, 293 say explicitly that no size is stated, 61 say nothing.
The 119 are a free, high-confidence recovery; the enricher's rule against inventing values kept them out of the
field because the number appeared in a paper or README rather than a header. Every one of the 7 prepared datasets
has a documented size, so no served crop carries an assumed box yet.

### 3.3 Channels (480 null, 231 inconsistent)

`imaging.channels` disagrees with the raw array in 231 records. The common patterns: `channels: 1` on a `yxs` array
with 3 samples (78 records: a gray image stored as RGB, or "essentially one dominant hue" as one record's notes
put it), `channels: 3` on a 4-sample array (28: RGBA), `channels: 2` on a plain `yx` or `zyx` array (39: the
channels are separate files, so the array lists one). The planner reads the array, which is right for the first
two patterns and under-counts the third. The gray recipe for a 3-sample brightfield or histology image is
luminance, and for a 3-sample fluorescence image the mean; a one-hue RGB fluorescence image wants a single
channel, which only the pixels reveal. `labels_stains` names the stains on most fluorescence records and could
name the channel order where the array has a `c` axis.

### 3.4 Organism and modality for the dataset folder (736 records excluded, 36%)

mia-data names a dataset folder `<family>-<organism>-<name>` and stops when the family or the organism is
unclear; the planner applies the same rule before planning. The 733 exclusions by that rule (743 in all with the 10
licence ones): 236 no organism with a clear modality, 163 several organisms with a clear modality, 138
no organism and modality `other`, 89 several organisms and modality `other`, 75 several organisms and several
modalities, 27 no organism and several modalities, 5 modality `other` alone.

Who has no organism: materials science (irradiated alloys, catalyst nanoparticles, carbon fibre, shale, mudrock),
environmental samples (phytoplankton, pollen, food surfaces), protein crystals, simulated data, and cell lines
whose species the harvester did not resolve ("DCIS.com Lifeact-RFP cells (organism not stated)"). The first groups
have no organism to infer; the folder rule needs a non-organism bucket for them. Cell lines resolve to a species
with high confidence from a lookup table (HEK, HeLa, U2OS are human; CHO is hamster). Multi-organism sets
(`empiar-11037-cem-mitolab-mitochondria` lists mouse, fly, worm and human) usually keep one organism per
sub-folder, which `layout.tree` shows; the catalog's own readiness tool takes an `--organism` override for them.

Modality `other` (241 records) is mostly SEM and STEM of non-biological material, infrared and Raman imaging, and
flow-cytometry imaging. These are not mis-classified; the enum lacks their modality. Inferring a family for them
means choosing one, and `em` for SEM of shale would mislead the voxel-size table (8 nm) as much as help the folder.

### 3.5 Licence

3 records are `unknown`, 89 `custom`. Ten records carry an SPDX id that neither the web UI's licence list nor the
example specs include (CC-BY-SA-3.0, CC-BY-3.0-US, CC-BY-1.0, CC-BY-ND-3.0, PDDL-1.0, CDLA-Permissive-1.0,
GPL-2.0-or-later, etalab-2.0, CC-BY-NC-2.0, CC-BY-NC-SA-2.0). That is a list to extend, not a catalog gap.

### 3.6 Download pointers

`data.download_url` is a direct file or bulk protocol on 603 records, the landing page itself on 467, another page
or an API on 692, and null on 291. Sample URLs exist on 1,844 records. Hosts: zenodo.org 854, ftp.ebi.ac.uk 202,
ndownloader.figshare.com 107, the COSEM S3 bucket 75, huggingface.co 61, data.mendeley.com 47. mia-data resolves
files itself from these, and of the 49 datasets it handed to a human, 46 failed on the job's network policy
(figshare and Hugging Face redirect to S3 or a CDN that is not on the allow list, Mendeley and OSF hosts not
allowed) and 2 on the record (no sourceable label; a `zcyx` raw array the snippet could not read). So the record's
pointer quality has not been the limiting factor yet; the host policy has. The 291 null and 467 landing-page
pointers will matter once the policy is widened, and the repository APIs resolve about 1,370 of them deterministically.

### 3.7 Size and counts

`size_bytes` null on 54; `size_source` is a complete file listing on 1,491, page-stated on 340, estimated on 143,
unknown on 57, and `listing_complete` is false on 271. `n_items` null on 96; 256 records are single-item.
Estimates come from `tools/estimate_size.py` (files times voxels times bytes per voxel) and are good to a factor
of two or so. The keeper's quota is in bytes, and the plan's positions use `n_items`, so both run on these.

### 3.8 Annotations

`annotations.present` is true with no array of role `label` on 356 records: the labels live in a table (COCO or
VIA JSON, CSV boxes or points) or were never inspected. Source is `unknown` on 315 and coverage `unknown` on 105;
label encoding `unknown` on 180 arrays and `other` on 277; alignment `unknown` on 263 arrays, and 237 are
scaled, cropped, offset or need a transform. 103 records say annotations are present but
`verification.annotations_verified` is false. A spec that requires annotations and a source passes these on the
record's word; mia-data's binding is where the label is found and keyed, with the reasons written down. The
binding is also where the `annotations.source` → label key mapping (manual, proofread, automatic) is settled, so a
record with source `unknown` costs a human review at `data:review` rather than a planner rejection.

### 3.9 Identity

403 records (20%) have no DOI: 125 `other`, 67 HuggingFace, 60 lab websites, 36 BioImage Archive, 21 each GitHub,
Kaggle and OpenOrganelle, 20 OSF. `dedup.by: [doi]` is blind to duplicates among them. The catalog's own dedup
tool keys on repository plus accession, URL, title and the paper's DOI; the planner has `path`, `landing_url` and
`publications[].doi` available and uses none of them. The cost is a dataset that enters the pool twice under two
records and is weighted twice.

### 3.10 Facts that live only in prose

The record's `notes`, `technical.notes` and `sample.description` carry what the enricher could not put in a
field, and they are honest about it: "not stated" or an equivalent appears in 1,277 records, "one sample only"
or "single file" in 893 (a value range or shape measured on one file), "inferred", "likely" or "presumably" in
397, "not opened" or "not downloaded" in 381, "estimated" in 195, "timed out" in 131, "approximation" in 59.
These are the catalog's confidence statements, and no field carries them. `provenance.confidence` is the
probability that the stated fields are correct and the dataset is usable, not that the record is complete: a
record with null voxel size and null axes can be 0.8 (the distribution: 0.5 on 34 records, 0.6 on 445, 0.7 on 598,
0.8 on 961, 0.9 on 15). `technical.method` grades the evidence behind the technical block better: `sample`
(files downloaded and measured) on 1,652, `header` (range requests) on 284, `metadata-only` on 101, absent on 16.

### 3.11 Internal consistency

- 9 records filed as 2D carry a z axis in their raw array and 15 carry a t axis; 75 records filed as 3D have no z
  axis in their largest raw array (the plane-per-file stacks above, plus 3 with `t` as the stack axis).
- `imaging.channels` disagrees with the array on 231 records (§3.3).
- 16 records are schema 1.1 with no `technical` block.
- The `assume.py` table is dated `ab6a013d`; two entries have drifted from the current medians (§3.2).
- The example spec `buffer-live-test.example.yaml` pins `ab6a013d`, two catalog refs behind.

## 4. What the planner says over the whole catalog

A permissive spec (all four dimensionalities, patch 256 with z 32, levels 1,2,4, every licence in use, annotations
optional, no dedup) against the 2,053 records:

| outcome | records | note |
|---|---|---|
| excluded before planning: no mia-data folder | 733 | §3.4 |
| excluded: licence not in the list | 10 | §3.5 |
| footprint 1,024 by 1,024 exceeds the largest item | 548 | 292 would fit at levels 1,2 (512), 146 at level 1 only, 110 are smaller than one 256 patch |
| no array shape | 219 | §3.1 (269 in the catalog; 50 of them fall to the folder rule first) |
| too few z slices for a 32-deep patch | 14 | |
| fits, prepared | 1 | |
| fits, not prepared | 528 | the ceiling of the pool today, 26% of the catalog |
| with `assume_voxel_size: false`: voxel size unknown | 251 of those 529 | 210 of them 2D |

Two of the three largest buckets are not catalog gaps. The folder rule is a naming convention that could admit a
non-organism bucket; the footprint rule is the spec's choice of levels, and a per-dataset level cap (serve a small
set at levels 1,2 and a large one at 1,2,4) would admit 438 more records. The shape gap is the catalog's, and a
third of it is recoverable by reading the shape's length.

## 5. Recommendations, in order of return on effort

1. **Infer axes from shape length in `specmap`, marked like assumed voxel sizes.** 2-long is yx; 3-long on a 3D
   record is zyx; 3-long ending in 3 or 4 is yxs. 111 records enter planning; the plan row says "axes inferred".
   High confidence; a header read at staging confirms or refutes it before any crop is served.
2. **Parse a stated pixel size out of prose** ("N nm/pixel", "N nm per pixel", "N µm/px") into the voxel-size
   fallback before the median table. 119 records move from an assumed to a stated size. High confidence; the
   regular expression must reject magnifications and lengths without "per pixel".
3. **Take z from the file count** for 3D records whose raw axes are yx, when `layout.tree` shows one folder of
   planes; otherwise leave the record out as now. 80 candidates, medium confidence, marked on the row.
4. **Feed preparation back.** mia-data settles axes, shape, voxel size, channel meaning, label keys, encodings and
   alignment with written reasons, and every crop measures the source. The spec's evaluation step (§4.3 of the
   prep-and-buffering spec) is the place; until it exists, write the binding's settled axes, shape and voxel size
   into the registry's `records` row so the planner reads the prepared facts over the catalog's. The prepared
   B. subtilis set would then plan on its 80 by 1,024 by 1,024 stack instead of "no array shape".
5. **Grade the voxel-size table by modality** and let a spec refuse low-confidence assumptions: the row already
   says "assumed"; add the modality's within-factor-two share (§3.2) so a spec can say `assume_voxel_size:
   {min_confidence: 0.7}` and admit EM at an assumed size while rejecting phase-contrast at one. Refresh the table
   from the current ref, where spinning-disk and DIC have drifted.
6. **A non-organism bucket in the folder rule** (`material`, `environmental`, `simulated`, `cell-line`) and a cell
   line to species table admit most of the 402 organism-less records; multi-organism sets need a per-file-set
   organism, which `layout.tree` usually shows and the catalog's readiness tool already accepts as an override.
7. **Dedup on more than the DOI**: `landing_url`, `repository` plus `accession`, and `publications[].doi`, the
   way the catalog's own `tools/dedup.py` does, so 403 DOI-less records are not invisible to it.
8. **Extend the licence list** in the web UI and the example specs with the ten SPDX ids in use.
9. **Per-dataset level cap** in the plan, so a 512-pixel dataset is served at levels 1,2 instead of rejected;
   438 records become eligible with no catalog change. This is a spec and tensorizer decision, not an inference.
10. **Upstream, for the catalog's enricher**: the 116 null-axes shapes and the 377 magnification-only records are
    its highest-yield follow-ups, and a `confidence` per inferred field (or at least per `technical` block) would
    let the planner weigh the record instead of reading the prose flags in §3.10.
