import type {Zero} from '@rocicorp/zero';
import {useQuery} from '@rocicorp/zero/react';
import {type Schema} from '../../shared/schema.ts';
import type {Mutators} from '../../shared/mutators.ts';
import {queries} from '../../shared/queries.ts';
import {useLogin} from './use-login.tsx';

export function useUserPref(key: string): string | undefined {
  const login = useLogin();
  const [pref] = useQuery(queries.userPref(login.loginState?.decoded, key));
  return pref?.value;
}

export async function setUserPref(
  z: Zero<Schema, Mutators>,
  key: string,
  value: string,
  mutate = z.mutate,
): Promise<void> {
  await mutate.userPref.set({key, value});
}

export function useNumericPref(key: string, defaultValue: number): number {
  const value = useUserPref(key);
  return value !== undefined ? parseInt(value, 10) : defaultValue;
}

export function setNumericPref(
  z: Zero<Schema, Mutators>,
  key: string,
  value: number,
): Promise<void> {
  return setUserPref(z, key, value + '');
}
