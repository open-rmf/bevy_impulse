/**
 * A trick to make typescript checks that the `switch` statement covers all cases.
 *
 * This works by taking an argument of type `never`, which will only be true when all cases are covered.
 *
 * # Example
 *
 * ```ts
 * switch (value) {
 *   case 'valueA':
 *     break;
 *   case 'valueB':
 *     break;
 *   default:
 *     exhaustiveCheck(type)
 * }
 * ```
 */
export function exhaustiveCheck(_: never) {}
