## 0.0.25

### Various fixes & improvements

- feat(chain): add ParquetSerializer (#140) by @victoria-yining-huang
- fix: replace freedthreaded_python with initialize_python everywhere (#174) by @bmckerry
- feat(rust_arroyo): add Broadcast step (#164) by @bmckerry
- Add kcat command to batching.py (#163) by @untitaker
- fix(ci): Also add release tags (#168) by @untitaker
- ref(rust): Use python for instance check when parsing error in PythonAdapter (#166) by @john-z-yang
- release: 0.0.24 (25aadbfa) by @getsentry-bot
- ref(rust): Re-enable tests (#167) by @john-z-yang
- Fix the process to append steps to the pipeline (#165) by @fpacifici
- feat(docs): add some Arroyo runtime docs (#161) by @bmckerry
- fix(uv): Pin python versions (#158) by @untitaker
- feat(watermark): add PyWatermarkMessage for passing watermark to python (#156) by @bmckerry
- temp: ignore failing tests (#159) by @bmckerry
- ref(rust): Parse and handle errors when interfacing with python code (#157) by @john-z-yang
- ref: Handle overrides in GCSSink from config (#153) by @ayirr7
- Chain primitives in Arroyo (#135) by @fpacifici
- ref(rust): fixup clippy lint (#154) by @john-z-yang
- fix(flatmap): Change the FlatMap signature to match usage (#151) by @evanh
- fix(docs): Update the docs for first time users (#142) by @evanh
- feat(logging): make traced_with_gil display callsite filename and lineno (#149) by @john-z-yang
- fix(examples): Migrate billing example (#146) by @evanh
- feat(chain): add BatchParser (#134) by @victoria-yining-huang
- feat(watermarks): track last seen message committable (#137) by @bmckerry
- temp: comment out failing tests (#150) by @bmckerry

_Plus 95 more_

## 0.0.24

### Various fixes & improvements

- ref(rust): Re-enable tests (#167) by @john-z-yang
- Fix the process to append steps to the pipeline (#165) by @fpacifici
- feat(docs): add some Arroyo runtime docs (#161) by @bmckerry
- fix(uv): Pin python versions (#158) by @untitaker
- feat(watermark): add PyWatermarkMessage for passing watermark to python (#156) by @bmckerry
- temp: ignore failing tests (#159) by @bmckerry
- ref(rust): Parse and handle errors when interfacing with python code (#157) by @john-z-yang
- ref: Handle overrides in GCSSink from config (#153) by @ayirr7
- Chain primitives in Arroyo (#135) by @fpacifici
- ref(rust): fixup clippy lint (#154) by @john-z-yang
- fix(flatmap): Change the FlatMap signature to match usage (#151) by @evanh
- fix(docs): Update the docs for first time users (#142) by @evanh
- feat(logging): make traced_with_gil display callsite filename and lineno (#149) by @john-z-yang
- fix(examples): Migrate billing example (#146) by @evanh
- feat(chain): add BatchParser (#134) by @victoria-yining-huang
- feat(watermarks): track last seen message committable (#137) by @bmckerry
- temp: comment out failing tests (#150) by @bmckerry
- fix(uv): Regenerate uv.lock using v0.7.13 (#148) by @evanh
- ref(error): Add MessageInvalidException to API and implement support in Map (#141) by @john-z-yang
- feat: Introduce object file generator for GCSSink (#144) by @ayirr7
- rename test_msg_parser file to test_msg_codecs (#145) by @victoria-yining-huang
- feat(rust_arroyo): add watermark step (#133) by @bmckerry
- release: 0.0.23 (430c5760) by @getsentry-bot
- Support parallelism in python (#132) by @fpacifici

_Plus 88 more_

## 0.0.23

### Various fixes & improvements

- Support parallelism in python (#132) by @fpacifici
- add example config (#138) by @victoria-yining-huang
- rename file (#139) by @victoria-yining-huang
- Generalize ReduceDelegate (#120) by @fpacifici
- quick-fix: Ensure schema is retained after a batching step (#131) by @ayirr7
- expose batch_size in config (#129) by @victoria-yining-huang
- release: 0.0.22 (e47bd9fb) by @getsentry-bot
- remove except (#130) by @victoria-yining-huang
- add datetime serilaizer (#128) by @victoria-yining-huang
- release: 0.0.21 (3f22b125) by @getsentry-bot
- feat(GCS-sink): Use RunTaskInThreads to write to GCS (#126) by @ayirr7
- release: 0.0.20 (0525b706) by @getsentry-bot
- fix extension-module (#125) by @fpacifici
- fix(gcs-writer): Better Error handling  (#124) by @ayirr7
- release: 0.0.19 (c58921b4) by @getsentry-bot
- Avoid bumping version number in uv.lock during release (#122) by @fpacifici
- Support Reduce in the Rust Adapter as a Python delegate (#119) by @fpacifici
- Allow re-initialization of the RustOperatorDelegate (#118) by @fpacifici
- Create and manage the Message in Rust (#121) by @fpacifici
- feat: Simple Single-threaded GCSSink step (#115) by @ayirr7
- Move the Message data structure to Rust (#116) by @fpacifici
- ref(ci): Disable extension-module by default (#113) by @untitaker
- Add a Python Delegate strategy to implement Streaming steps in python (#107) by @fpacifici
- fix(ci): Bump uv.lock as part of release flow (#114) by @untitaker

_Plus 65 more_

## 0.0.22

### Various fixes & improvements

- remove except (#130) by @victoria-yining-huang
- add datetime serilaizer (#128) by @victoria-yining-huang
- release: 0.0.21 (3f22b125) by @getsentry-bot
- feat(GCS-sink): Use RunTaskInThreads to write to GCS (#126) by @ayirr7
- release: 0.0.20 (0525b706) by @getsentry-bot
- fix extension-module (#125) by @fpacifici
- fix(gcs-writer): Better Error handling  (#124) by @ayirr7
- release: 0.0.19 (c58921b4) by @getsentry-bot
- Avoid bumping version number in uv.lock during release (#122) by @fpacifici
- Support Reduce in the Rust Adapter as a Python delegate (#119) by @fpacifici
- Allow re-initialization of the RustOperatorDelegate (#118) by @fpacifici
- Create and manage the Message in Rust (#121) by @fpacifici
- feat: Simple Single-threaded GCSSink step (#115) by @ayirr7
- Move the Message data structure to Rust (#116) by @fpacifici
- ref(ci): Disable extension-module by default (#113) by @untitaker
- Add a Python Delegate strategy to implement Streaming steps in python (#107) by @fpacifici
- fix(ci): Bump uv.lock as part of release flow (#114) by @untitaker
- add (#112) by @victoria-yining-huang
- add (#109) by @victoria-yining-huang
- try (#108) by @victoria-yining-huang
- copy paste (#110) by @victoria-yining-huang
- release: 0.0.18 (c65580a8) by @getsentry-bot
- feat: Basic filter step for the Rust adapter (#105) by @ayirr7
- Support routing in the Rust adapter (#104) by @fpacifici

_Plus 58 more_

## 0.0.21

### Various fixes & improvements

- feat(GCS-sink): Use RunTaskInThreads to write to GCS (#126) by @ayirr7
- release: 0.0.20 (0525b706) by @getsentry-bot
- fix extension-module (#125) by @fpacifici
- fix(gcs-writer): Better Error handling  (#124) by @ayirr7
- release: 0.0.19 (c58921b4) by @getsentry-bot
- Avoid bumping version number in uv.lock during release (#122) by @fpacifici
- Support Reduce in the Rust Adapter as a Python delegate (#119) by @fpacifici
- Allow re-initialization of the RustOperatorDelegate (#118) by @fpacifici
- Create and manage the Message in Rust (#121) by @fpacifici
- feat: Simple Single-threaded GCSSink step (#115) by @ayirr7
- Move the Message data structure to Rust (#116) by @fpacifici
- ref(ci): Disable extension-module by default (#113) by @untitaker
- Add a Python Delegate strategy to implement Streaming steps in python (#107) by @fpacifici
- fix(ci): Bump uv.lock as part of release flow (#114) by @untitaker
- add (#112) by @victoria-yining-huang
- add (#109) by @victoria-yining-huang
- try (#108) by @victoria-yining-huang
- copy paste (#110) by @victoria-yining-huang
- release: 0.0.18 (c65580a8) by @getsentry-bot
- feat: Basic filter step for the Rust adapter (#105) by @ayirr7
- Support routing in the Rust adapter (#104) by @fpacifici
- release: 0.0.17 (0078dae8) by @getsentry-bot
- Pypi seems not to like an empty readme (#103) by @fpacifici
- Add workflow to build the wheel containing the rust binary (#102) by @fpacifici

_Plus 55 more_

## 0.0.20

### Various fixes & improvements

- fix extension-module (#125) by @fpacifici
- fix(gcs-writer): Better Error handling  (#124) by @ayirr7
- release: 0.0.19 (c58921b4) by @getsentry-bot
- Avoid bumping version number in uv.lock during release (#122) by @fpacifici
- Support Reduce in the Rust Adapter as a Python delegate (#119) by @fpacifici
- Allow re-initialization of the RustOperatorDelegate (#118) by @fpacifici
- Create and manage the Message in Rust (#121) by @fpacifici
- feat: Simple Single-threaded GCSSink step (#115) by @ayirr7
- Move the Message data structure to Rust (#116) by @fpacifici
- ref(ci): Disable extension-module by default (#113) by @untitaker
- Add a Python Delegate strategy to implement Streaming steps in python (#107) by @fpacifici
- fix(ci): Bump uv.lock as part of release flow (#114) by @untitaker
- add (#112) by @victoria-yining-huang
- add (#109) by @victoria-yining-huang
- try (#108) by @victoria-yining-huang
- copy paste (#110) by @victoria-yining-huang
- release: 0.0.18 (c65580a8) by @getsentry-bot
- feat: Basic filter step for the Rust adapter (#105) by @ayirr7
- Support routing in the Rust adapter (#104) by @fpacifici
- release: 0.0.17 (0078dae8) by @getsentry-bot
- Pypi seems not to like an empty readme (#103) by @fpacifici
- Add workflow to build the wheel containing the rust binary (#102) by @fpacifici
- Rust Arroyo adapter (#98) by @fpacifici
- ref: Type out chains and steps (#99) by @ayirr7

_Plus 53 more_

## 0.0.19

### Various fixes & improvements

- Avoid bumping version number in uv.lock during release (#122) by @fpacifici
- Support Reduce in the Rust Adapter as a Python delegate (#119) by @fpacifici
- Allow re-initialization of the RustOperatorDelegate (#118) by @fpacifici
- Create and manage the Message in Rust (#121) by @fpacifici
- feat: Simple Single-threaded GCSSink step (#115) by @ayirr7
- Move the Message data structure to Rust (#116) by @fpacifici
- ref(ci): Disable extension-module by default (#113) by @untitaker
- Add a Python Delegate strategy to implement Streaming steps in python (#107) by @fpacifici
- fix(ci): Bump uv.lock as part of release flow (#114) by @untitaker
- add (#112) by @victoria-yining-huang
- add (#109) by @victoria-yining-huang
- try (#108) by @victoria-yining-huang
- copy paste (#110) by @victoria-yining-huang
- release: 0.0.18 (c65580a8) by @getsentry-bot
- feat: Basic filter step for the Rust adapter (#105) by @ayirr7
- Support routing in the Rust adapter (#104) by @fpacifici
- release: 0.0.17 (0078dae8) by @getsentry-bot
- Pypi seems not to like an empty readme (#103) by @fpacifici
- Add workflow to build the wheel containing the rust binary (#102) by @fpacifici
- Rust Arroyo adapter (#98) by @fpacifici
- ref: Type out chains and steps (#99) by @ayirr7
- feat: Flink deployment config (#90) by @ayirr7
- feat: add arroyo broadcast step (#91) by @bmckerry
- fix: Update config types for bootstrap_servers (#97) by @ayirr7

_Plus 50 more_

## 0.0.18

### Various fixes & improvements

- feat: Basic filter step for the Rust adapter (#105) by @ayirr7
- Support routing in the Rust adapter (#104) by @fpacifici
- release: 0.0.17 (0078dae8) by @getsentry-bot
- Pypi seems not to like an empty readme (#103) by @fpacifici
- Add workflow to build the wheel containing the rust binary (#102) by @fpacifici
- Rust Arroyo adapter (#98) by @fpacifici
- ref: Type out chains and steps (#99) by @ayirr7
- feat: Flink deployment config (#90) by @ayirr7
- feat: add arroyo broadcast step (#91) by @bmckerry
- fix: Update config types for bootstrap_servers (#97) by @ayirr7
- fix: arroyo adapter deployment config (#96) by @bmckerry
- release: 0.0.16 (16cb4330) by @getsentry-bot
- ref: Make Flink tests use deployment config (#95) by @ayirr7
- Fix typo (#94) by @fpacifici
- Add some docs (#93) by @fpacifici
- feat: Arroyo-backed SlidingWindows, TumblingWindows (#85) by @ayirr7
- release: 0.0.15 (44332cb7) by @getsentry-bot
- feat: Add deployment config (#77) by @ayirr7
- Add docs (#69) by @ayirr7
- feat: add explicit broadcast step to flink (#89) by @bmckerry
- release: 0.0.14 (0f41d356) by @getsentry-bot
- feat: add explicit Broadcast step to pipeline graph (#88) by @bmckerry
- Add multi chain support (#86) by @fpacifici
- Introduce chained primitives to replace the old DSL (#82) by @fpacifici

_Plus 36 more_

## 0.0.17

### Various fixes & improvements

- Pypi seems not to like an empty readme (#103) by @fpacifici
- Add workflow to build the wheel containing the rust binary (#102) by @fpacifici
- Rust Arroyo adapter (#98) by @fpacifici
- ref: Type out chains and steps (#99) by @ayirr7
- feat: Flink deployment config (#90) by @ayirr7
- feat: add arroyo broadcast step (#91) by @bmckerry
- fix: Update config types for bootstrap_servers (#97) by @ayirr7
- fix: arroyo adapter deployment config (#96) by @bmckerry
- release: 0.0.16 (16cb4330) by @getsentry-bot
- ref: Make Flink tests use deployment config (#95) by @ayirr7
- Fix typo (#94) by @fpacifici
- Add some docs (#93) by @fpacifici
- feat: Arroyo-backed SlidingWindows, TumblingWindows (#85) by @ayirr7
- release: 0.0.15 (44332cb7) by @getsentry-bot
- feat: Add deployment config (#77) by @ayirr7
- Add docs (#69) by @ayirr7
- feat: add explicit broadcast step to flink (#89) by @bmckerry
- release: 0.0.14 (0f41d356) by @getsentry-bot
- feat: add explicit Broadcast step to pipeline graph (#88) by @bmckerry
- Add multi chain support (#86) by @fpacifici
- Introduce chained primitives to replace the old DSL (#82) by @fpacifici
- feat: router for arroyo adapter (#83) by @bmckerry
- ref: remove topics mapping from env config (#84) by @bmckerry
- feat: add router to sentry_flink (#64) by @bmckerry

_Plus 33 more_

## 0.0.16

### Various fixes & improvements

- ref: Make Flink tests use deployment config (#95) by @ayirr7
- Fix typo (#94) by @fpacifici
- Add some docs (#93) by @fpacifici
- feat: Arroyo-backed SlidingWindows, TumblingWindows (#85) by @ayirr7
- release: 0.0.15 (44332cb7) by @getsentry-bot
- feat: Add deployment config (#77) by @ayirr7
- Add docs (#69) by @ayirr7
- feat: add explicit broadcast step to flink (#89) by @bmckerry
- release: 0.0.14 (0f41d356) by @getsentry-bot
- feat: add explicit Broadcast step to pipeline graph (#88) by @bmckerry
- Add multi chain support (#86) by @fpacifici
- Introduce chained primitives to replace the old DSL (#82) by @fpacifici
- feat: router for arroyo adapter (#83) by @bmckerry
- ref: remove topics mapping from env config (#84) by @bmckerry
- feat: add router to sentry_flink (#64) by @bmckerry
- fix(flink): rename KafkaSink/KafkaSource/generics (#81) by @bmckerry
- release: 0.0.13 (cb59c4fa) by @getsentry-bot
- fix: rename Stream/StreamSink generics (#80) by @bmckerry
- release: 0.0.12 (3a98c93b) by @getsentry-bot
- Make streams less Kafkaesque (#79) by @fpacifici
- feat: add router primitive to sentry_streams (#61) by @bmckerry
- Adapte the runner to run the Arroyo adapter (#78) by @fpacifici
- feat: Add Batching, FlatMap support in Flink (#63) by @ayirr7
- release: 0.0.11 (10938dc8) by @getsentry-bot

_Plus 24 more_

## 0.0.15

### Various fixes & improvements

- feat: Add deployment config (#77) by @ayirr7
- Add docs (#69) by @ayirr7
- feat: add explicit broadcast step to flink (#89) by @bmckerry
- release: 0.0.14 (0f41d356) by @getsentry-bot
- feat: add explicit Broadcast step to pipeline graph (#88) by @bmckerry
- Add multi chain support (#86) by @fpacifici
- Introduce chained primitives to replace the old DSL (#82) by @fpacifici
- feat: router for arroyo adapter (#83) by @bmckerry
- ref: remove topics mapping from env config (#84) by @bmckerry
- feat: add router to sentry_flink (#64) by @bmckerry
- fix(flink): rename KafkaSink/KafkaSource/generics (#81) by @bmckerry
- release: 0.0.13 (cb59c4fa) by @getsentry-bot
- fix: rename Stream/StreamSink generics (#80) by @bmckerry
- release: 0.0.12 (3a98c93b) by @getsentry-bot
- Make streams less Kafkaesque (#79) by @fpacifici
- feat: add router primitive to sentry_streams (#61) by @bmckerry
- Adapte the runner to run the Arroyo adapter (#78) by @fpacifici
- feat: Add Batching, FlatMap support in Flink (#63) by @ayirr7
- release: 0.0.11 (10938dc8) by @getsentry-bot
- ref: Remove Unbatch, flesh out Reduce ABC (#70) by @ayirr7
- Add an Arroyo Adapter (#68) by @fpacifici
- release: 0.0.10 (223d4b5c) by @getsentry-bot
- feat: Add a Batch, Unbatch, FlatMap primitive (#62) by @ayirr7
- release: 0.0.9 (f4063d22) by @getsentry-bot

_Plus 19 more_

## 0.0.14

### Various fixes & improvements

- feat: add explicit Broadcast step to pipeline graph (#88) by @bmckerry
- Add multi chain support (#86) by @fpacifici
- Introduce chained primitives to replace the old DSL (#82) by @fpacifici
- feat: router for arroyo adapter (#83) by @bmckerry
- ref: remove topics mapping from env config (#84) by @bmckerry
- feat: add router to sentry_flink (#64) by @bmckerry
- fix(flink): rename KafkaSink/KafkaSource/generics (#81) by @bmckerry
- release: 0.0.13 (cb59c4fa) by @getsentry-bot
- fix: rename Stream/StreamSink generics (#80) by @bmckerry
- release: 0.0.12 (3a98c93b) by @getsentry-bot
- Make streams less Kafkaesque (#79) by @fpacifici
- feat: add router primitive to sentry_streams (#61) by @bmckerry
- Adapte the runner to run the Arroyo adapter (#78) by @fpacifici
- feat: Add Batching, FlatMap support in Flink (#63) by @ayirr7
- release: 0.0.11 (10938dc8) by @getsentry-bot
- ref: Remove Unbatch, flesh out Reduce ABC (#70) by @ayirr7
- Add an Arroyo Adapter (#68) by @fpacifici
- release: 0.0.10 (223d4b5c) by @getsentry-bot
- feat: Add a Batch, Unbatch, FlatMap primitive (#62) by @ayirr7
- release: 0.0.9 (f4063d22) by @getsentry-bot
- ref(sentry_streams): add function to TransformStep class (#60) by @victoria-yining-huang
- fix(env): Fix global envrc, one envrc per subproject (#66) by @untitaker
- feat(api): add broadcast functionality (#32) by @bmckerry
- add more unittests for Pipeline class (#56) by @victoria-yining-huang

_Plus 15 more_

## 0.0.13

### Various fixes & improvements

- fix: rename Stream/StreamSink generics (#80) by @bmckerry
- release: 0.0.12 (3a98c93b) by @getsentry-bot
- Make streams less Kafkaesque (#79) by @fpacifici
- feat: add router primitive to sentry_streams (#61) by @bmckerry
- Adapte the runner to run the Arroyo adapter (#78) by @fpacifici
- feat: Add Batching, FlatMap support in Flink (#63) by @ayirr7
- release: 0.0.11 (10938dc8) by @getsentry-bot
- ref: Remove Unbatch, flesh out Reduce ABC (#70) by @ayirr7
- Add an Arroyo Adapter (#68) by @fpacifici
- release: 0.0.10 (223d4b5c) by @getsentry-bot
- feat: Add a Batch, Unbatch, FlatMap primitive (#62) by @ayirr7
- release: 0.0.9 (f4063d22) by @getsentry-bot
- ref(sentry_streams): add function to TransformStep class (#60) by @victoria-yining-huang
- fix(env): Fix global envrc, one envrc per subproject (#66) by @untitaker
- feat(api): add broadcast functionality (#32) by @bmckerry
- add more unittests for Pipeline class (#56) by @victoria-yining-huang
- fix: Remove extra function declarations (#58) by @ayirr7
- release: 0.0.8 (42a87850) by @getsentry-bot
- fix: sentry_streams lib structure (#57) by @ayirr7
- release: 0.0.7 (06db3f7e) by @getsentry-bot
- feat: Add windowing and aggregation (#18) by @ayirr7
- release: 0.0.6 (4493198e) by @getsentry-bot
- ref: split user_functions (#53) by @bmckerry
- add unittest to Pipeline class (#52) by @victoria-yining-huang

_Plus 7 more_

## 0.0.12

### Various fixes & improvements

- Make streams less Kafkaesque (#79) by @fpacifici
- feat: add router primitive to sentry_streams (#61) by @bmckerry
- Adapte the runner to run the Arroyo adapter (#78) by @fpacifici
- feat: Add Batching, FlatMap support in Flink (#63) by @ayirr7
- release: 0.0.11 (10938dc8) by @getsentry-bot
- ref: Remove Unbatch, flesh out Reduce ABC (#70) by @ayirr7
- Add an Arroyo Adapter (#68) by @fpacifici
- release: 0.0.10 (223d4b5c) by @getsentry-bot
- feat: Add a Batch, Unbatch, FlatMap primitive (#62) by @ayirr7
- release: 0.0.9 (f4063d22) by @getsentry-bot
- ref(sentry_streams): add function to TransformStep class (#60) by @victoria-yining-huang
- fix(env): Fix global envrc, one envrc per subproject (#66) by @untitaker
- feat(api): add broadcast functionality (#32) by @bmckerry
- add more unittests for Pipeline class (#56) by @victoria-yining-huang
- fix: Remove extra function declarations (#58) by @ayirr7
- release: 0.0.8 (42a87850) by @getsentry-bot
- fix: sentry_streams lib structure (#57) by @ayirr7
- release: 0.0.7 (06db3f7e) by @getsentry-bot
- feat: Add windowing and aggregation (#18) by @ayirr7
- release: 0.0.6 (4493198e) by @getsentry-bot
- ref: split user_functions (#53) by @bmckerry
- add unittest to Pipeline class (#52) by @victoria-yining-huang
- release: 0.0.5 (781d1a93) by @getsentry-bot
- Fix the package lookup (#50) by @fpacifici

_Plus 5 more_

## 0.0.11

### Various fixes & improvements

- ref: Remove Unbatch, flesh out Reduce ABC (#70) by @ayirr7
- Add an Arroyo Adapter (#68) by @fpacifici
- release: 0.0.10 (223d4b5c) by @getsentry-bot
- feat: Add a Batch, Unbatch, FlatMap primitive (#62) by @ayirr7
- release: 0.0.9 (f4063d22) by @getsentry-bot
- ref(sentry_streams): add function to TransformStep class (#60) by @victoria-yining-huang
- fix(env): Fix global envrc, one envrc per subproject (#66) by @untitaker
- feat(api): add broadcast functionality (#32) by @bmckerry
- add more unittests for Pipeline class (#56) by @victoria-yining-huang
- fix: Remove extra function declarations (#58) by @ayirr7
- release: 0.0.8 (42a87850) by @getsentry-bot
- fix: sentry_streams lib structure (#57) by @ayirr7
- release: 0.0.7 (06db3f7e) by @getsentry-bot
- feat: Add windowing and aggregation (#18) by @ayirr7
- release: 0.0.6 (4493198e) by @getsentry-bot
- ref: split user_functions (#53) by @bmckerry
- add unittest to Pipeline class (#52) by @victoria-yining-huang
- release: 0.0.5 (781d1a93) by @getsentry-bot
- Fix the package lookup (#50) by @fpacifici
- release: 0.0.4 (a1ddabc8) by @getsentry-bot
- Remove setuptools (#49) by @fpacifici
- Fix path to bump verison (#46) by @fpacifici
- Move changelog (#39) by @fpacifici
- Add releaseing code (#35) by @fpacifici

## 0.0.10

### Various fixes & improvements

- feat: Add a Batch, Unbatch, FlatMap primitive (#62) by @ayirr7
- release: 0.0.9 (f4063d22) by @getsentry-bot
- ref(sentry_streams): add function to TransformStep class (#60) by @victoria-yining-huang
- fix(env): Fix global envrc, one envrc per subproject (#66) by @untitaker
- feat(api): add broadcast functionality (#32) by @bmckerry
- add more unittests for Pipeline class (#56) by @victoria-yining-huang
- fix: Remove extra function declarations (#58) by @ayirr7
- release: 0.0.8 (42a87850) by @getsentry-bot
- fix: sentry_streams lib structure (#57) by @ayirr7
- release: 0.0.7 (06db3f7e) by @getsentry-bot
- feat: Add windowing and aggregation (#18) by @ayirr7
- release: 0.0.6 (4493198e) by @getsentry-bot
- ref: split user_functions (#53) by @bmckerry
- add unittest to Pipeline class (#52) by @victoria-yining-huang
- release: 0.0.5 (781d1a93) by @getsentry-bot
- Fix the package lookup (#50) by @fpacifici
- release: 0.0.4 (a1ddabc8) by @getsentry-bot
- Remove setuptools (#49) by @fpacifici
- Fix path to bump verison (#46) by @fpacifici
- Move changelog (#39) by @fpacifici
- Add releaseing code (#35) by @fpacifici

## 0.0.9

### Various fixes & improvements

- ref(sentry_streams): add function to TransformStep class (#60) by @victoria-yining-huang
- fix(env): Fix global envrc, one envrc per subproject (#66) by @untitaker
- feat(api): add broadcast functionality (#32) by @bmckerry
- add more unittests for Pipeline class (#56) by @victoria-yining-huang
- fix: Remove extra function declarations (#58) by @ayirr7
- release: 0.0.8 (42a87850) by @getsentry-bot
- fix: sentry_streams lib structure (#57) by @ayirr7
- release: 0.0.7 (06db3f7e) by @getsentry-bot
- feat: Add windowing and aggregation (#18) by @ayirr7
- release: 0.0.6 (4493198e) by @getsentry-bot
- ref: split user_functions (#53) by @bmckerry
- add unittest to Pipeline class (#52) by @victoria-yining-huang
- release: 0.0.5 (781d1a93) by @getsentry-bot
- Fix the package lookup (#50) by @fpacifici
- release: 0.0.4 (a1ddabc8) by @getsentry-bot
- Remove setuptools (#49) by @fpacifici
- Fix path to bump verison (#46) by @fpacifici
- Move changelog (#39) by @fpacifici
- Add releaseing code (#35) by @fpacifici

## 0.0.8

### Various fixes & improvements

- fix: sentry_streams lib structure (#57) by @ayirr7
- release: 0.0.7 (06db3f7e) by @getsentry-bot
- feat: Add windowing and aggregation (#18) by @ayirr7
- release: 0.0.6 (4493198e) by @getsentry-bot
- ref: split user_functions (#53) by @bmckerry
- add unittest to Pipeline class (#52) by @victoria-yining-huang
- release: 0.0.5 (781d1a93) by @getsentry-bot
- Fix the package lookup (#50) by @fpacifici
- release: 0.0.4 (a1ddabc8) by @getsentry-bot
- Remove setuptools (#49) by @fpacifici
- Fix path to bump verison (#46) by @fpacifici
- Move changelog (#39) by @fpacifici
- Add releaseing code (#35) by @fpacifici

## 0.0.7

### Various fixes & improvements

- feat: Add windowing and aggregation (#18) by @ayirr7
- release: 0.0.6 (4493198e) by @getsentry-bot
- ref: split user_functions (#53) by @bmckerry
- add unittest to Pipeline class (#52) by @victoria-yining-huang
- release: 0.0.5 (781d1a93) by @getsentry-bot
- Fix the package lookup (#50) by @fpacifici
- release: 0.0.4 (a1ddabc8) by @getsentry-bot
- Remove setuptools (#49) by @fpacifici
- Fix path to bump verison (#46) by @fpacifici
- Move changelog (#39) by @fpacifici
- Add releaseing code (#35) by @fpacifici

## 0.0.6

### Various fixes & improvements

- ref: split user_functions (#53) by @bmckerry
- add unittest to Pipeline class (#52) by @victoria-yining-huang
- release: 0.0.5 (781d1a93) by @getsentry-bot
- Fix the package lookup (#50) by @fpacifici
- release: 0.0.4 (a1ddabc8) by @getsentry-bot
- Remove setuptools (#49) by @fpacifici
- Fix path to bump verison (#46) by @fpacifici
- Move changelog (#39) by @fpacifici
- Add releaseing code (#35) by @fpacifici

## 0.0.5

### Various fixes & improvements

- Fix the package lookup (#50) by @fpacifici
- release: 0.0.4 (a1ddabc8) by @getsentry-bot
- Remove setuptools (#49) by @fpacifici
- Fix path to bump verison (#46) by @fpacifici
- Move changelog (#39) by @fpacifici
- Add releaseing code (#35) by @fpacifici

## 0.0.4

### Various fixes & improvements

- Remove setuptools (#49) by @fpacifici
- Fix path to bump verison (#46) by @fpacifici
- Move changelog (#39) by @fpacifici
- Add releaseing code (#35) by @fpacifici

