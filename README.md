### Implementation notes

Latest commit is an improvement over original implementation.

Changelog:

- Fixed duplicate CSV header in output.
- Added deposit and withdrawal amount validation.
- Added output rounding (regarding precision: not sure if validation to ensure input has at most 4
  decimal places is needed).
- Added more test cases.
- More of the logic expressed in type system. For example, can only apply "persisted" transactions
  to client.
- Used checked arithmetic operations.
