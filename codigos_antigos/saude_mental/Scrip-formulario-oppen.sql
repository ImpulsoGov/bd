with ajuste_polaridade_escala as (
select
	V0101,
	case
		when V0010_1 = 1 then 0.25
		when V0010_1 = 0 then 0.0
		else null
	end +
case
		when V0010_2 = 1 then 0.25
		when V0010_2 = 0 then 0.0
		else null
	end +
case
		when V0010_3 = 1 then 0.25
		when V0010_3 = 0 then 0.0
		else null
	end +
case
		when V0010_4 = 1 then 0.25
		when V0010_4 = 0 then 0.0
		else null
	end +
case
		when V0010_77 = 1 then 0.0
		when V0010_77 = 0 then 0.0
		else null
	end +
case
		when V0010_99 = 1 then null
		when V0010_99 = 0 then 0.0
		else null
	end as V0010_ajustada,
	case
		when V0011 = 1 then 0
		when V0011 = 2 then 0.25
		when V0011 = 3 then 0.5
		when V0011 = 4 then 0.75
		when V0011 = 5 then 1
		when V0011 = 77 then 0
		when V0011 = 99 then null
		else null
	end as V0011_ajustada,
	case
		when V0012_1 = 1 then 0.0
		when V0012_1 = 0 then 0.143
		else null
	end +
case
		when V0012_2 = 1 then 0.0
		when V0012_2 = 0 then 0.143
		else null
	end +
case
		when V0012_3 = 1 then 0.0
		when V0012_3 = 0 then 0.143
		else null
	end +
case
		when V0012_4 = 1 then 0.0
		when V0012_4 = 0 then 0.143
		else null
	end +
case
		when V0012_5 = 1 then 0.0
		when V0012_5 = 0 then 0.143
		else null
	end +
case
		when V0012_6 = 1 then 0.0
		when V0012_6 = 0 then 0.143
		else null
	end +
case
		when V0012_7 = 1 then 0.0
		when V0012_7 = 0 then 0.143
		else null
	end +
case
		when V0012_77 = 1 then 0.0
		when V0012_77 = 0 then 0.0
		else null
	end +
case
		when V0012_99 = 1 then null
		when V0012_99 = 0 then 0.0
		else null
	end as V0012_ajustada,
	case
		when V0013 = 1 then 1
		when V0013 = 2 then 1
		when V0013 = 3 then 0.5
		when V0013 = 4 then 0.25
		when V0013 = 5 then 0.25
		when V0013 = 77 then 0
		when V0013 = 99 then null
		else null
	end as V0013_ajustada,
	case
		when V0014 = 1 then 0
		when V0014 = 2 then 0.25
		when V0014 = 3 then 0.5
		when V0014 = 4 then 0.75
		when V0014 = 5 then 1
		when V0014 = 77 then 0
		when V0014 = 99 then null
		else null
	end as V0014_ajustada,
	case
		when V0015A = 2 then 0.25
		when V0015A = 3 then 0.5
		when V0015A = 4 then 0.75
		when V0015A = 5 then 1
		when V0015A = 77 then 0
		when V0015A = 99 then null
		else null
	end as V0015A_ajustada,
	case
		when V0015B = 1 then 0
		when V0015B = 2 then 0.25
		when V0015B = 3 then 0.5
		when V0015B = 4 then 0.75
		when V0015B = 5 then 1
		when V0015B = 77 then 0
		when V0015B = 99 then null
		else null
	end as V0015B_ajustada,
	case
		when V0015C = 1 then 0
		when V0015C = 2 then 0.25
		when V0015C = 3 then 0.5
		when V0015C = 4 then 0.75
		when V0015C = 5 then 1
		when V0015C = 77 then 0
		when V0015C = 99 then null
		else null
	end as V0015C_ajustada,
	case
		when V0015D = 1 then 0
		when V0015D = 2 then 0.25
		when V0015D = 3 then 0.5
		when V0015D = 4 then 0.75
		when V0015D = 5 then 1
		when V0015D = 77 then 0
		when V0015D = 99 then null
		else null
	end as V0015D_ajustada,
	case
		when V0016 = 1 then 0
		when V0016 = 2 then 0.25
		when V0016 = 3 then 0.5
		when V0016 = 4 then 0.75
		when V0016 = 5 then 1
		when V0016 = 77 then 0
		when V0016 = 99 then null
		else null
	end as V0016_ajustada,
	case
		when V0017A = 1 then 0
		when V0017A = 2 then 0.25
		when V0017A = 3 then 0.5
		when V0017A = 4 then 0.75
		when V0017A = 5 then 1
		when V0017A = 77 then 0
		when V0017A = 99 then null
		else null
	end as V0017A_ajustada,
	case
		when V0017B = 1 then 0
		when V0017B = 2 then 0.25
		when V0017B = 3 then 0.5
		when V0017B = 4 then 0.75
		when V0017B = 5 then 1
		when V0017B = 77 then 0
		when V0017B = 99 then null
		else null
	end as V0017B_ajustada,
	case
		when V0017C = 1 then 0
		when V0017C = 2 then 0.25
		when V0017C = 3 then 0.5
		when V0017C = 4 then 0.75
		when V0017C = 5 then 1
		when V0017C = 77 then 0
		when V0017C = 99 then null
		else null
	end as V0017C_ajustada,
	case
		when V0017D = 1 then 0
		when V0017D = 2 then 0.25
		when V0017D = 3 then 0.5
		when V0017D = 4 then 0.75
		when V0017D = 5 then 1
		when V0017D = 77 then 0
		when V0017D = 99 then null
		else null
	end as V0017D_ajustada,
	case
		when V0018 = 1 then 0.0
		when V0018 = 2 then 0.25
		when V0018 = 3 then 0.5
		when V0018 = 4 then 0.75
		when V0018 = 5 then 1.0
		when V0018 = 77 then 0.0
		else null
	end as V0018_ajustada,
	case
		when V0019 = 1 then 0.0
		when V0019 = 2 then 0.25
		when V0019 = 3 then 0.5
		when V0019 = 4 then 0.75
		when V0019 = 5 then 1.0
		when V0019 = 77 then 0.0
		else null
	end as V0019_ajustada,
	case
		when V0020A = 1 then 0
		when V0020A = 2 then 0.25
		when V0020A = 4 then 0.75
		when V0020A = 5 then 1
		when V0020A = 77 then 0
		when V0020A = 99 then null
		else null
	end as V0020A_ajustada,
	case
		when V0020B = 1 then 0
		when V0020B = 2 then 0.25
		when V0020B = 3 then 0.5
		when V0020B = 4 then 0.75
		when V0020B = 5 then 1
		when V0020B = 77 then 0
		when V0020B = 99 then null
		else null
	end as V0020B_ajustada,
	case
		when V0020C = 1 then 0
		when V0020C = 2 then 0.25
		when V0020C = 3 then 0.5
		when V0020C = 4 then 0.75
		when V0020C = 5 then 1
		when V0020C = 77 then 0
		when V0020C = 99 then null
		else null
	end as V0020C_ajustada,
	case
		when V0020D = 1 then 0
		when V0020D = 2 then 0.25
		when V0020D = 3 then 0.5
		when V0020D = 4 then 0.75
		when V0020D = 5 then 1
		when V0020D = 77 then 0
		when V0020D = 99 then null
		else null
	end as V0020D_ajustada,
	case
		when V0020E = 1 then 0
		when V0020E = 2 then 0.25
		when V0020E = 3 then 0.5
		when V0020E = 4 then 0.75
		when V0020E = 5 then 1
		when V0020E = 77 then 0
		when V0020E = 99 then null
		else null
	end as V0020E_ajustada,
	case
		when V0023 = 1 then 0
		when V0023 = 2 then 0.25
		when V0023 = 3 then 0.5
		when V0023 = 4 then 0.75
		when V0023 = 5 then 1
		when V0023 = 77 then 0
		when V0023 = 99 then null
		else null
	end as V0023_ajustada,
	case
		when V0024 = 1 then 0
		when V0024 = 2 then 0.25
		when V0024 = 3 then 0.5
		when V0024 = 4 then 0.75
		when V0024 = 5 then 1
		when V0024 = 77 then 0
		when V0024 = 99 then null
		else null
	end as V0024_ajustada,
	case
		when V0025 = 1 then 0
		when V0025 = 2 then 0.25
		when V0025 = 3 then 0.5
		when V0025 = 4 then 0.75
		when V0025 = 5 then 1
		when V0025 = 77 then 0
		when V0025 = 99 then null
		else null
	end as V0025_ajustada,
	case
		when V0025A = 1 then 0
		when V0025A = 2 then 0.25
		when V0025A = 3 then 0.5
		when V0025A = 4 then 0.75
		when V0025A = 5 then 1
		when V0025A = 77 then 0
		when V0025A = 99 then null
		else null
	end as V0025A_ajustada,
	case
		when V0025C_1 = 1 then 0.1667
		when V0025C_1 = 0 then 0
		else null
	end +
case
		when V0025C_2 = 1 then 0.1667
		when V0025C_2 = 0 then 0
		else null
	end +
case
		when V0025C_3 = 1 then 0.1667
		when V0025C_3 = 0 then 0
		else null
	end +
case
		when V0025C_4 = 1 then 0.1667
		when V0025C_4 = 0 then 0
		else null
	end +
case
		when V0025C_5 = 1 then 0.1667
		when V0025C_5 = 0 then 0
		else null
	end +
case
		when V0025C_6 = 1 then 0.1667
		when V0025C_6 = 0 then 0
		else null
	end +
case
		when V0025C_7 = 1 then 0
		when V0025C_7 = 0 then 0
		else null
	end +
case
		when V0025C_77 = 1 then 0
		when V0025C_77 = 0 then 0
		else null
	end +
case
		when V0025C_99 = 1 then null
		when V0025C_99 = 0 then 0
		else null
	end as V0025C_ajustada,
	case
		when V0026 = 1 then 0
		when V0026 = 2 then 0.25
		when V0026 = 3 then 0.5
		when V0026 = 4 then 0.75
		when V0026 = 5 then 1
		when V0026 = 77 then 0
		when V0026 = 99 then null
		else null
	end as V0026_ajustada,
	case
		when V0027A = 1 then 0
		when V0027A = 2 then 0.25
		when V0027A = 3 then 0.5
		when V0027A = 4 then 0.75
		when V0027A = 5 then 1
		when V0027A = 77 then 0
		when V0027A = 99 then null
		else null
	end as V0027A_ajustada,
	case
		when V0028A = 1 then 0
		when V0028A = 2 then 0.25
		when V0028A = 3 then 0.5
		when V0028A = 4 then 0.75
		when V0028A = 5 then 1
		when V0028A = 99 then null
		else null
	end as V0028A_ajustada,
	case
		when V0028B = 1 then 0
		when V0028B = 2 then 0.25
		when V0028B = 3 then 0.5
		when V0028B = 4 then 0.75
		when V0028B = 5 then 1
		when V0028B = 77 then 0
		when V0028B = 99 then null
		else null
	end as V0028B_ajustada,
	case
		when V0028C = 1 then 0
		when V0028C = 2 then 0.25
		when V0028C = 3 then 0.5
		when V0028C = 4 then 0.75
		when V0028C = 5 then 1
		when V0028C = 77 then 0
		when V0028C = 99 then null
		else null
	end as V0028C_ajustada,
	case
		when V0028D = 1 then 0
		when V0028D = 2 then 0.25
		when V0028D = 3 then 0.5
		when V0028D = 4 then 0.75
		when V0028D = 5 then 1
		when V0028D = 77 then 0
		when V0028D = 99 then null
		else null
	end as V0028D_ajustada,
	case
		when V0028E = 1 then 0
		when V0028E = 2 then 0.25
		when V0028E = 3 then 0.5
		when V0028E = 4 then 0.75
		when V0028E = 5 then 1
		when V0028E = 77 then 0
		when V0028E = 99 then null
		else null
	end as V0028E_ajustada,
	case
		when V0028F = 1 then 0
		when V0028F = 2 then 0.25
		when V0028F = 3 then 0.5
		when V0028F = 4 then 0.75
		when V0028F = 5 then 1
		when V0028F = 77 then 0
		when V0028F = 99 then null
		else null
	end as V0028F_ajustada,
	case
		when V0028G = 1 then 0
		when V0028G = 2 then 0.25
		when V0028G = 3 then 0.5
		when V0028G = 4 then 0.75
		when V0028G = 5 then 1
		when V0028G = 77 then 0
		when V0028G = 99 then null
		else null
	end as V0028G_ajustada,
	case
		when V0030A = 1 then 0
		when V0030A = 2 then 0.25
		when V0030A = 3 then 0.5
		when V0030A = 4 then 0.75
		when V0030A = 5 then 1
		when V0030A = 77 then 0
		when V0030A = 99 then null
		else null
	end as V0030A_ajustada,
	case
		when V0030B = 1 then 0
		when V0030B = 2 then 0.25
		when V0030B = 3 then 0.5
		when V0030B = 4 then 0.75
		when V0030B = 5 then 1
		when V0030B = 77 then 0
		when V0030B = 99 then null
		else null
	end as V0030B_ajustada,
	case
		when V0030C = 1 then 0
		when V0030C = 2 then 0.25
		when V0030C = 3 then 0.5
		when V0030C = 4 then 0.75
		when V0030C = 5 then 1
		when V0030C = 77 then 0
		when V0030C = 99 then null
		else null
	end as V0030C_ajustada,
	case
		when V0031 = 1 then 0
		when V0031 = 2 then 0.25
		when V0031 = 3 then 0.5
		when V0031 = 4 then 0.75
		when V0031 = 5 then 1
		when V0031 = 77 then 0
		when V0031 = 99 then null
		else null
	end as V0031_ajustada,
	case
		when V0031B = 1 then 0
		when V0031B = 2 then 0.25
		when V0031B = 3 then 0.5
		when V0031B = 4 then 0.75
		when V0031B = 5 then 1
		when V0031B = 77 then 0
		when V0031B = 99 then null
		else null
	end as V0031B_ajustada,
	case
		when V0032A = 1 then 0
		when V0032A = 2 then 0.25
		when V0032A = 3 then 0.5
		when V0032A = 4 then 0.75
		when V0032A = 5 then 1
		when V0032A = 77 then 0
		when V0032A = 99 then null
		else null
	end as V0032A_ajustada,
	case
		when V0032B = 1 then 0
		when V0032B = 2 then 0.25
		when V0032B = 3 then 0.5
		when V0032B = 4 then 0.75
		when V0032B = 5 then 1
		when V0032B = 77 then 0
		when V0032B = 99 then null
		else null
	end as V0032B_ajustada,
	case
		when V0032D = 1 then 0
		when V0032D = 2 then 0.25
		when V0032D = 3 then 0.5
		when V0032D = 4 then 0.75
		when V0032D = 5 then 1
		when V0032D = 77 then 0
		when V0032D = 99 then null
		else null
	end as V0032D_ajustada,
	case
		when V0033 = 1 then 0
		when V0033 = 2 then 0.25
		when V0033 = 3 then 0.5
		when V0033 = 4 then 0.75
		when V0033 = 5 then 1
		when V0033 = 77 then 0
		when V0033 = 99 then null
		else null
	end as V0033_ajustada,
	case
		when V0034 = 1 then 0
		when V0034 = 2 then 0.25
		when V0034 = 3 then 0.5
		when V0034 = 4 then 0.75
		when V0034 = 5 then 1
		when V0034 = 77 then 0
		when V0034 = 99 then null
		else null
	end as V0034_ajustada,
	case
		when V0035_1 = 1 then 0.2
		when V0035_1 = 0 then 0
		else null
	end +
case
		when V0035_2 = 1 then 0.2
		when V0035_2 = 0 then 0
		else null
	end +
case
		when V0035_3 = 1 then 0.2
		when V0035_3 = 0 then 0
		else null
	end +
case
		when V0035_4 = 1 then 0.2
		when V0035_4 = 0 then 0
		else null
	end +
case
		when V0035_5 = 1 then 0.2
		when V0035_5 = 0 then 0
		else null
	end +
case
		when V0035_6 = 1 then 0.2
		when V0035_6 = 0 then 0
		else null
	end +
case
		when V0035_77 = 1 then 0
		when V0035_77 = 0 then 0
		else null
	end +
case
		when V0035_99 = 1 then null
		when V0035_99 = 0 then 0
		else null
	end as V0035_ajustada,
	case
		when V0036 = 1 then 0
		when V0036 = 2 then 0.25
		when V0036 = 3 then 0.5
		when V0036 = 4 then 0.75
		when V0036 = 5 then 1
		when V0036 = 77 then 0
		when V0036 = 99 then null
		else null
	end as V0036_ajustada,
	case
		when V0037 = 1 then 0
		when V0037 = 2 then 0.25
		when V0037 = 3 then 0.5
		when V0037 = 4 then 0.75
		when V0037 = 5 then 1
		when V0037 = 77 then 0
		when V0037 = 99 then null
		else null
	end as V0037_ajustada,
	case
		when V0038 = 1 then 0
		when V0038 = 2 then 0.25
		when V0038 = 3 then 0.5
		when V0038 = 4 then 0.75
		when V0038 = 5 then 1
		when V0038 = 77 then 0
		when V0038 = 99 then null
		else null
	end as V0038_ajustada,
	case
		when V0039 = 1 then 0
		when V0039 = 2 then 0.25
		when V0039 = 3 then 0.5
		when V0039 = 4 then 0.75
		when V0039 = 5 then 1
		when V0039 = 77 then 0
		when V0039 = 99 then null
		else null
	end as V0039_ajustada,
	case
		when V0041_1 = 1 then 0.2
		when V0041_1 = 0 then 0
		else null
	end +
case
		when V0041_2 = 1 then 0.2
		when V0041_2 = 0 then 0
		else null
	end +
case
		when V0041_3 = 1 then 0.2
		when V0041_3 = 0 then 0
		else null
	end +
case
		when V0041_4 = 1 then 0.2
		when V0041_4 = 0 then 0
		else null
	end +
case
		when V0041_5 = 1 then 0.2
		when V0041_5 = 0 then 0
		else null
	end +
case
		when V0041_6 = 1 then 0
		when V0041_6 = 0 then 0
		else null
	end +
case
		when V0041_77 = 1 then 0
		when V0041_77 = 0 then 0
		else null
	end +
case
		when V0041_99 = 1 then null
		when V0041_99 = 0 then 0
		else null
	end as V0041_ajustada,
	case
		when V0042 = 1 then 0
		when V0042 = 2 then 0.25
		when V0042 = 3 then 0.5
		when V0042 = 4 then 0.75
		when V0042 = 5 then 1
		when V0042 = 77 then 0
		when V0042 = 99 then null
		else null
	end as V0042_ajustada,
	case
		when V0043 = 1 then 0
		when V0043 = 2 then 0.25
		when V0043 = 3 then 0.5
		when V0043 = 4 then 0.75
		when V0043 = 5 then 1
		when V0043 = 77 then 0
		when V0043 = 99 then null
		else null
	end as V0043_ajustada,
	case
		when V0044 = 1 then 0
		when V0044 = 2 then 0.25
		when V0044 = 3 then 0.5
		when V0044 = 4 then 0.75
		when V0044 = 5 then 1
		when V0044 = 77 then 0
		when V0044 = 99 then null
		else null
	end as V0044_ajustada,
	case
		when V0045_1 = 1 then 0.25
		when V0045_1 = 0 then 0
		else null
	end +
case
		when V0045_2 = 1 then 0.25
		when V0045_2 = 0 then 0
		else null
	end +
case
		when V0045_3 = 1 then 0.25
		when V0045_3 = 0 then 0
		else null
	end +
case
		when V0045_4 = 1 then 0.25
		when V0045_4 = 0 then 0
		else null
	end +
case
		when V0045_77 = 1 then 0
		when V0045_77 = 0 then 0
		else null
	end +
case
		when V0045_99 = 1 then null
		when V0045_99 = 0 then 0
		else null
	end as V0045_ajustada,
	case
		when V0046A = 1 then 0
		when V0046A = 2 then 0.25
		when V0046A = 3 then 0.5
		when V0046A = 4 then 0.75
		when V0046A = 5 then 1
		when V0046A = 77 then 0
		when V0046A = 99 then null
		else null
	end as V0046A_ajustada,
	case
		when V0046B = 1 then 0
		when V0046B = 2 then 0.25
		when V0046B = 3 then 0.5
		when V0046B = 4 then 0.75
		when V0046B = 5 then 1
		when V0046B = 99 then null
		else null
	end as V0046B_ajustada,
	case
		when V0047 = 1 then 0
		when V0047 = 2 then 0.25
		when V0047 = 3 then 0.5
		when V0047 = 4 then 0.75
		when V0047 = 5 then 1
		when V0047 = 77 then 0
		when V0047 = 99 then null
		else null
	end as V0047_ajustada,
	case
		when V0048 = 1 then 0
		when V0048 = 2 then 0.25
		when V0048 = 3 then 0.5
		when V0048 = 4 then 0.75
		when V0048 = 5 then 1
		when V0048 = 77 then 0
		when V0048 = 99 then null
		else null
	end as V0048_ajustada,
	case
		when V0049 = 1 then 0
		when V0049 = 2 then 0.25
		when V0049 = 3 then 0.5
		when V0049 = 4 then 0.75
		when V0049 = 5 then 1
		when V0049 = 77 then 0
		when V0049 = 99 then null
		else null
	end as V0049_ajustada,
	case
		when V0050_1 = 1 then 0.25
		when V0050_1 = 0 then 0
		else null
	end +
case
		when V0050_2 = 1 then 0.25
		when V0050_2 = 0 then 0
		else null
	end +
case
		when V0050_3 = 1 then 0.25
		when V0050_3 = 0 then 0
		else null
	end +
case
		when V0050_4 = 1 then 0.25
		when V0050_4 = 0 then 0
		else null
	end +
case
		when V0050_77 = 1 then 0
		when V0050_77 = 0 then 0
		else null
	end +
case
		when V0050_99 = 1 then null
		when V0050_99 = 0 then 0
		else null
	end as V0050_ajustada,
	case
		when V0051 = 1 then 0
		when V0051 = 2 then 0.25
		when V0051 = 3 then 0.5
		when V0051 = 4 then 0.75
		when V0051 = 5 then 1
		when V0051 = 77 then 0
		when V0051 = 99 then null
		else null
	end as V0051_ajustada,
	case
		when V0052 = 1 then 0
		when V0052 = 2 then 0.25
		when V0052 = 3 then 0.5
		when V0052 = 4 then 0.75
		when V0052 = 5 then 1
		when V0052 = 77 then 0
		when V0052 = 99 then null
		else null
	end as V0052_ajustada,
	case
		when V0054A = 1 then 0
		when V0054A = 2 then 0.25
		when V0054A = 3 then 0.5
		when V0054A = 4 then 0.75
		when V0054A = 5 then 1
		when V0054A = 77 then 0
		when V0054A = 99 then null
		else null
	end as V0054A_ajustada,
	case
		when V0054B = 1 then 0
		when V0054B = 2 then 0.25
		when V0054B = 3 then 0.5
		when V0054B = 4 then 0.75
		when V0054B = 5 then 1
		when V0054B = 77 then 0
		when V0054B = 99 then null
		else null
	end as V0054B_ajustada,
	case
		when V0054C = 1 then 0
		when V0054C = 2 then 0.25
		when V0054C = 3 then 0.5
		when V0054C = 4 then 0.75
		when V0054C = 5 then 1
		when V0054C = 77 then 0
		when V0054C = 99 then null
		else null
	end as V0054C_ajustada,
	case
		when V0054D = 1 then 0
		when V0054D = 2 then 0.25
		when V0054D = 3 then 0.5
		when V0054D = 4 then 0.75
		when V0054D = 5 then 1
		when V0054D = 77 then 0
		when V0054D = 99 then null
		else null
	end as V0054D_ajustada,
	case
		when V0055 = 1 then 0
		when V0055 = 2 then 0.25
		when V0055 = 3 then 0.5
		when V0055 = 4 then 0.75
		when V0055 = 5 then 1
		when V0055 = 77 then 0
		when V0055 = 99 then null
		else null
	end as V0055_ajustada,
	case
		when V0055A = 1 then 0
		when V0055A = 2 then 0.25
		when V0055A = 3 then 0.5
		when V0055A = 4 then 0.75
		when V0055A = 5 then 1
		when V0055A = 77 then 0
		when V0055A = 99 then null
		else null
	end as V0055A_ajustada,
	case
		when V0055B = 1 then 0
		when V0055B = 2 then 0.25
		when V0055B = 3 then 0.5
		when V0055B = 4 then 0.75
		when V0055B = 5 then 1
		when V0055B = 77 then 0
		when V0055B = 99 then null
		else null
	end as V0055B_ajustada,
	case
		when V0056 = 1 then 0
		when V0056 = 2 then 0.25
		when V0056 = 3 then 0.5
		when V0056 = 4 then 0.75
		when V0056 = 5 then 1
		when V0056 = 77 then 0
		when V0056 = 99 then null
		else null
	end as V0056_ajustada,
	case
		when V0056A = 1 then 0
		when V0056A = 2 then 0.25
		when V0056A = 3 then 0.5
		when V0056A = 4 then 0.75
		when V0056A = 5 then 1
		when V0056A = 77 then 0
		when V0056A = 99 then null
		else null
	end as V0056A_ajustada,
	case
		when V0057A = 1 then 0
		when V0057A = 2 then 0.25
		when V0057A = 3 then 0.5
		when V0057A = 4 then 0.75
		when V0057A = 5 then 1
		when V0057A = 77 then 0
		when V0057A = 99 then null
		else null
	end as V0057A_ajustada,
	case
		when V0057B = 1 then 0
		when V0057B = 2 then 0.25
		when V0057B = 3 then 0.5
		when V0057B = 4 then 0.75
		when V0057B = 5 then 1
		when V0057B = 77 then 0
		when V0057B = 99 then null
		else null
	end as V0057B_ajustada,
	case
		when V0057C = 1 then 0
		when V0057C = 2 then 0.25
		when V0057C = 3 then 0.5
		when V0057C = 4 then 0.75
		when V0057C = 5 then 1
		when V0057C = 77 then 0
		when V0057C = 99 then null
		else null
	end as V0057C_ajustada,
	case
		when V0057D = 1 then 0
		when V0057D = 2 then 0.25
		when V0057D = 3 then 0.5
		when V0057D = 4 then 0.75
		when V0057D = 5 then 1
		when V0057D = 77 then 0
		when V0057D = 99 then null
		else null
	end as V0057D_ajustada,
	case
		when V0058_1 = 1 then 0.142857142857143
		when V0058_1 = 0 then 0
		else null
	end +
case
		when V0058_2 = 1 then 0.142857142857143
		when V0058_2 = 0 then 0
		else null
	end +
case
		when V0058_3 = 1 then 0.142857142857143
		when V0058_3 = 0 then 0
		else null
	end +
case
		when V0058_4 = 1 then 0.142857142857143
		when V0058_4 = 0 then 0
		else null
	end +
case
		when V0058_5 = 1 then 0.142857142857143
		when V0058_5 = 0 then 0
		else null
	end +
case
		when V0058_6 = 1 then 0.142857142857143
		when V0058_6 = 0 then 0
		else null
	end +
case
		when V0058_7 = 1 then 0.142857142857143
		when V0058_7 = 0 then 0
		else null
	end +
case
		when V0058_77 = 1 then 0
		when V0058_77 = 0 then 0
		else null
	end +
case
		when V0058_99 = 1 then null
		when V0058_99 = 0 then 0
		else null
	end as V0058_ajustada,
	case
		when V0059_1 = 1 then 0.166666666666667
		when V0059_1 = 0 then 0
		else null
	end +
case
		when V0059_2 = 1 then 0.166666666666667
		when V0059_2 = 0 then 0
		else null
	end +
case
		when V0059_3 = 1 then 0.166666666666667
		when V0059_3 = 0 then 0
		else null
	end +
case
		when V0059_4 = 1 then 0.166666666666667
		when V0059_4 = 0 then 0
		else null
	end +
case
		when V0059_5 = 1 then 0.166666666666667
		when V0059_5 = 0 then 0
		else null
	end +
case
		when V0059_6 = 1 then 0.166666666666667
		when V0059_6 = 0 then 0
		else null
	end +
case
		when V0059_77 = 1 then 0
		when V0059_77 = 0 then 0
		else null
	end +
case
		when V0059_99 = 1 then null
		when V0059_99 = 0 then 0
		else null
	end as V0059_ajustada
from
	saude_mental."_formulario_oppen")
-----------------------------------------------------------------
,
media_por_rede as (
select
	V0101,
	AVG(V0010_ajustada) as V0010_ajustada,
	AVG(V0011_ajustada) as V0011_ajustada,
	AVG(V0012_ajustada) as V0012_ajustada,
	AVG(V0013_ajustada) as V0013_ajustada,
	AVG(V0014_ajustada) as V0014_ajustada,
	AVG(V0015A_ajustada) as V0015A_ajustada,
	AVG(V0015B_ajustada) as V0015B_ajustada,
	AVG(V0015C_ajustada) as V0015C_ajustada,
	AVG(V0015D_ajustada) as V0015D_ajustada,
	AVG(V0016_ajustada) as V0016_ajustada,
	AVG(V0017A_ajustada) as V0017A_ajustada,
	AVG(V0017B_ajustada) as V0017B_ajustada,
	AVG(V0017C_ajustada) as V0017C_ajustada,
	AVG(V0017D_ajustada) as V0017D_ajustada,
	AVG(V0018_ajustada) as V0018_ajustada,
	AVG(V0019_ajustada) as V0019_ajustada,
	AVG(V0020A_ajustada) as V0020A_ajustada,
	AVG(V0020B_ajustada) as V0020B_ajustada,
	AVG(V0020C_ajustada) as V0020C_ajustada,
	AVG(V0020D_ajustada) as V0020D_ajustada,
	AVG(V0020E_ajustada) as V0020E_ajustada,
	AVG(V0023_ajustada) as V0023_ajustada,
	AVG(V0024_ajustada) as V0024_ajustada,
	AVG(V0025_ajustada) as V0025_ajustada,
	AVG(V0025A_ajustada) as V0025A_ajustada,
	AVG(V0025C_ajustada) as V0025C_ajustada,
	AVG(V0026_ajustada) as V0026_ajustada,
	AVG(V0027A_ajustada) as V0027A_ajustada,
	AVG(V0028A_ajustada) as V0028A_ajustada,
	AVG(V0028B_ajustada) as V0028B_ajustada,
	AVG(V0028C_ajustada) as V0028C_ajustada,
	AVG(V0028D_ajustada) as V0028D_ajustada,
	AVG(V0028E_ajustada) as V0028E_ajustada,
	AVG(V0028F_ajustada) as V0028F_ajustada,
	AVG(V0028G_ajustada) as V0028G_ajustada,
	AVG(V0030A_ajustada) as V0030A_ajustada,
	AVG(V0030B_ajustada) as V0030B_ajustada,
	AVG(V0030C_ajustada) as V0030C_ajustada,
	AVG(V0031_ajustada) as V0031_ajustada,
	AVG(V0031B_ajustada) as V0031B_ajustada,
	AVG(V0032A_ajustada) as V0032A_ajustada,
	AVG(V0032B_ajustada) as V0032B_ajustada,
	AVG(V0032D_ajustada) as V0032D_ajustada,
	AVG(V0033_ajustada) as V0033_ajustada,
	AVG(V0034_ajustada) as V0034_ajustada,
	AVG(V0035_ajustada) as V0035_ajustada,
	AVG(V0036_ajustada) as V0036_ajustada,
	AVG(V0037_ajustada) as V0037_ajustada,
	AVG(V0038_ajustada) as V0038_ajustada,
	AVG(V0039_ajustada) as V0039_ajustada,
	AVG(V0041_ajustada) as V0041_ajustada,
	AVG(V0042_ajustada) as V0042_ajustada,
	AVG(V0043_ajustada) as V0043_ajustada,
	AVG(V0044_ajustada) as V0044_ajustada,
	AVG(V0045_ajustada) as V0045_ajustada,
	AVG(V0046A_ajustada) as V0046A_ajustada,
	AVG(V0046B_ajustada) as V0046B_ajustada,
	AVG(V0047_ajustada) as V0047_ajustada,
	AVG(V0048_ajustada) as V0048_ajustada,
	AVG(V0049_ajustada) as V0049_ajustada,
	AVG(V0050_ajustada) as V0050_ajustada,
	AVG(V0051_ajustada) as V0051_ajustada,
	AVG(V0052_ajustada) as V0052_ajustada,
	AVG(V0054A_ajustada) as V0054A_ajustada,
	AVG(V0054B_ajustada) as V0054B_ajustada,
	AVG(V0054C_ajustada) as V0054C_ajustada,
	AVG(V0054D_ajustada) as V0054D_ajustada,
	AVG(V0055_ajustada) as V0055_ajustada,
	AVG(V0055A_ajustada) as V0055A_ajustada,
	AVG(V0055B_ajustada) as V0055B_ajustada,
	AVG(V0056_ajustada) as V0056_ajustada,
	AVG(V0056A_ajustada) as V0056A_ajustada,
	AVG(V0057A_ajustada) as V0057A_ajustada,
	AVG(V0057B_ajustada) as V0057B_ajustada,
	AVG(V0057C_ajustada) as V0057C_ajustada,
	AVG(V0057D_ajustada) as V0057D_ajustada,
	AVG(V0058_ajustada) as V0058_ajustada,
	AVG(V0059_ajustada) as V0059_ajustada
from
	ajuste_polaridade_escala
group by
	V0101)
--------------------------------------
-- PARTE 3 ---------
-- CHAMAR media_por_rede
--------------------------------------
	,
denominadores as (
select
	V0101,
	case
		when V0010_ajustada is not null then V0010_ajustada
		else 0
	end as V0010_ajustada,
	case
		when V0010_ajustada is not null then 1
		else 0
	end as V0010_ajustada_1_denominador,
	case
		when V0011_ajustada is not null then V0011_ajustada
		else 0
	end as V0011_ajustada,
	case
		when V0011_ajustada is not null then 1
		else 0
	end as V0011_ajustada_1_denominador,
	case
		when V0012_ajustada is not null then V0012_ajustada
		else 0
	end as V0012_ajustada,
	case
		when V0012_ajustada is not null then 1
		else 0
	end as V0012_ajustada_1_denominador,
	case
		when V0013_ajustada is not null then V0013_ajustada
		else 0
	end as V0013_ajustada,
	case
		when V0013_ajustada is not null then 1
		else 0
	end as V0013_ajustada_1_denominador,
	case
		when V0014_ajustada is not null then V0014_ajustada
		else 0
	end as V0014_ajustada,
	case
		when V0014_ajustada is not null then 1
		else 0
	end as V0014_ajustada_1_denominador,
	case
		when V0015A_ajustada is not null then V0015A_ajustada
		else 0
	end as V0015A_ajustada,
	case
		when V0015A_ajustada is not null then 1
		else 0
	end as V0015A_ajustada_1_denominador,
	case
		when V0015B_ajustada is not null then V0015B_ajustada
		else 0
	end as V0015B_ajustada,
	case
		when V0015B_ajustada is not null then 1
		else 0
	end as V0015B_ajustada_1_denominador,
	case
		when V0015C_ajustada is not null then V0015C_ajustada
		else 0
	end as V0015C_ajustada,
	case
		when V0015C_ajustada is not null then 1
		else 0
	end as V0015C_ajustada_1_denominador,
	case
		when V0015D_ajustada is not null then V0015D_ajustada
		else 0
	end as V0015D_ajustada,
	case
		when V0015D_ajustada is not null then 1
		else 0
	end as V0015D_ajustada_1_denominador,
	case
		when V0016_ajustada is not null then V0016_ajustada
		else 0
	end as V0016_ajustada,
	case
		when V0016_ajustada is not null then 1
		else 0
	end as V0016_ajustada_1_denominador,
	case
		when V0017A_ajustada is not null then V0017A_ajustada
		else 0
	end as V0017A_ajustada,
	case
		when V0017A_ajustada is not null then 1
		else 0
	end as V0017A_ajustada_1_denominador,
	case
		when V0017B_ajustada is not null then V0017B_ajustada
		else 0
	end as V0017B_ajustada,
	case
		when V0017B_ajustada is not null then 1
		else 0
	end as V0017B_ajustada_1_denominador,
	case
		when V0017C_ajustada is not null then V0017C_ajustada
		else 0
	end as V0017C_ajustada,
	case
		when V0017C_ajustada is not null then 1
		else 0
	end as V0017C_ajustada_1_denominador,
	case
		when V0017D_ajustada is not null then V0017D_ajustada
		else 0
	end as V0017D_ajustada,
	case
		when V0017D_ajustada is not null then 1
		else 0
	end as V0017D_ajustada_1_denominador,
	case
		when V0018_ajustada is not null then V0018_ajustada
		else 0
	end as V0018_ajustada,
	case
		when V0018_ajustada is not null then 1
		else 0
	end as V0018_ajustada_1_denominador,
	case
		when V0019_ajustada is not null then V0019_ajustada
		else 0
	end as V0019_ajustada,
	case
		when V0019_ajustada is not null then 1
		else 0
	end as V0019_ajustada_1_denominador,
	case
		when V0020A_ajustada is not null then V0020A_ajustada
		else 0
	end as V0020A_ajustada,
	case
		when V0020A_ajustada is not null then 1
		else 0
	end as V0020A_ajustada_1_denominador,
	case
		when V0020B_ajustada is not null then V0020B_ajustada
		else 0
	end as V0020B_ajustada,
	case
		when V0020B_ajustada is not null then 1
		else 0
	end as V0020B_ajustada_1_denominador,
	case
		when V0020C_ajustada is not null then V0020C_ajustada
		else 0
	end as V0020C_ajustada,
	case
		when V0020C_ajustada is not null then 1
		else 0
	end as V0020C_ajustada_1_denominador,
	case
		when V0020D_ajustada is not null then V0020D_ajustada
		else 0
	end as V0020D_ajustada,
	case
		when V0020D_ajustada is not null then 1
		else 0
	end as V0020D_ajustada_1_denominador,
	case
		when V0020E_ajustada is not null then V0020E_ajustada
		else 0
	end as V0020E_ajustada,
	case
		when V0020E_ajustada is not null then 1
		else 0
	end as V0020E_ajustada_1_denominador,
	case
		when V0023_ajustada is not null then V0023_ajustada
		else 0
	end as V0023_ajustada,
	case
		when V0023_ajustada is not null then 1
		else 0
	end as V0023_ajustada_1_denominador,
	case
		when V0024_ajustada is not null then V0024_ajustada
		else 0
	end as V0024_ajustada,
	case
		when V0024_ajustada is not null then 1
		else 0
	end as V0024_ajustada_1_denominador,
	case
		when V0025_ajustada is not null then V0025_ajustada
		else 0
	end as V0025_ajustada,
	case
		when V0025_ajustada is not null then 1
		else 0
	end as V0025_ajustada_1_denominador,
	case
		when V0025A_ajustada is not null then V0025A_ajustada
		else 0
	end as V0025A_ajustada,
	case
		when V0025A_ajustada is not null then 1
		else 0
	end as V0025A_ajustada_1_denominador,
	case
		when V0025C_ajustada is not null then V0025C_ajustada
		else 0
	end as V0025C_ajustada,
	case
		when V0025C_ajustada is not null then 1
		else 0
	end as V0025C_ajustada_1_denominador,
	case
		when V0026_ajustada is not null then V0026_ajustada
		else 0
	end as V0026_ajustada,
	case
		when V0026_ajustada is not null then 1
		else 0
	end as V0026_ajustada_1_denominador,
	case
		when V0027A_ajustada is not null then V0027A_ajustada
		else 0
	end as V0027A_ajustada,
	case
		when V0027A_ajustada is not null then 1
		else 0
	end as V0027A_ajustada_1_denominador,
	case
		when V0028A_ajustada is not null then V0028A_ajustada
		else 0
	end as V0028A_ajustada,
	case
		when V0028A_ajustada is not null then 1
		else 0
	end as V0028A_ajustada_1_denominador,
	case
		when V0028B_ajustada is not null then V0028B_ajustada
		else 0
	end as V0028B_ajustada,
	case
		when V0028B_ajustada is not null then 1
		else 0
	end as V0028B_ajustada_1_denominador,
	case
		when V0028C_ajustada is not null then V0028C_ajustada
		else 0
	end as V0028C_ajustada,
	case
		when V0028C_ajustada is not null then 1
		else 0
	end as V0028C_ajustada_1_denominador,
	case
		when V0028D_ajustada is not null then V0028D_ajustada
		else 0
	end as V0028D_ajustada,
	case
		when V0028D_ajustada is not null then 1
		else 0
	end as V0028D_ajustada_1_denominador,
	case
		when V0028E_ajustada is not null then V0028E_ajustada
		else 0
	end as V0028E_ajustada,
	case
		when V0028E_ajustada is not null then 1
		else 0
	end as V0028E_ajustada_1_denominador,
	case
		when V0028F_ajustada is not null then V0028F_ajustada
		else 0
	end as V0028F_ajustada,
	case
		when V0028F_ajustada is not null then 1
		else 0
	end as V0028F_ajustada_1_denominador,
	case
		when V0028G_ajustada is not null then V0028G_ajustada
		else 0
	end as V0028G_ajustada,
	case
		when V0028G_ajustada is not null then 1
		else 0
	end as V0028G_ajustada_1_denominador,
	case
		when V0030A_ajustada is not null then V0030A_ajustada
		else 0
	end as V0030A_ajustada,
	case
		when V0030A_ajustada is not null then 1
		else 0
	end as V0030A_ajustada_1_denominador,
	case
		when V0030B_ajustada is not null then V0030B_ajustada
		else 0
	end as V0030B_ajustada,
	case
		when V0030B_ajustada is not null then 1
		else 0
	end as V0030B_ajustada_1_denominador,
	case
		when V0030C_ajustada is not null then V0030C_ajustada
		else 0
	end as V0030C_ajustada,
	case
		when V0030C_ajustada is not null then 1
		else 0
	end as V0030C_ajustada_1_denominador,
	case
		when V0031_ajustada is not null then V0031_ajustada
		else 0
	end as V0031_ajustada,
	case
		when V0031_ajustada is not null then 1
		else 0
	end as V0031_ajustada_1_denominador,
	case
		when V0031B_ajustada is not null then V0031B_ajustada
		else 0
	end as V0031B_ajustada,
	case
		when V0031B_ajustada is not null then 1
		else 0
	end as V0031B_ajustada_1_denominador,
	case
		when V0032A_ajustada is not null then V0032A_ajustada
		else 0
	end as V0032A_ajustada,
	case
		when V0032A_ajustada is not null then 1
		else 0
	end as V0032A_ajustada_1_denominador,
	case
		when V0032B_ajustada is not null then V0032B_ajustada
		else 0
	end as V0032B_ajustada,
	case
		when V0032B_ajustada is not null then 1
		else 0
	end as V0032B_ajustada_1_denominador,
	case
		when V0032D_ajustada is not null then V0032D_ajustada
		else 0
	end as V0032D_ajustada,
	case
		when V0032D_ajustada is not null then 1
		else 0
	end as V0032D_ajustada_1_denominador,
	case
		when V0033_ajustada is not null then V0033_ajustada
		else 0
	end as V0033_ajustada,
	case
		when V0033_ajustada is not null then 1
		else 0
	end as V0033_ajustada_1_denominador,
	case
		when V0034_ajustada is not null then V0034_ajustada
		else 0
	end as V0034_ajustada,
	case
		when V0034_ajustada is not null then 1
		else 0
	end as V0034_ajustada_1_denominador,
	case
		when V0035_ajustada is not null then V0035_ajustada
		else 0
	end as V0035_ajustada,
	case
		when V0035_ajustada is not null then 1
		else 0
	end as V0035_ajustada_1_denominador,
	case
		when V0036_ajustada is not null then V0036_ajustada
		else 0
	end as V0036_ajustada,
	case
		when V0036_ajustada is not null then 1
		else 0
	end as V0036_ajustada_1_denominador,
	case
		when V0037_ajustada is not null then V0037_ajustada
		else 0
	end as V0037_ajustada,
	case
		when V0037_ajustada is not null then 1
		else 0
	end as V0037_ajustada_1_denominador,
	case
		when V0038_ajustada is not null then V0038_ajustada
		else 0
	end as V0038_ajustada,
	case
		when V0038_ajustada is not null then 1
		else 0
	end as V0038_ajustada_1_denominador,
	case
		when V0039_ajustada is not null then V0039_ajustada
		else 0
	end as V0039_ajustada,
	case
		when V0039_ajustada is not null then 1
		else 0
	end as V0039_ajustada_1_denominador,
	case
		when V0041_ajustada is not null then V0041_ajustada
		else 0
	end as V0041_ajustada,
	case
		when V0041_ajustada is not null then 1
		else 0
	end as V0041_ajustada_1_denominador,
	case
		when V0042_ajustada is not null then V0042_ajustada
		else 0
	end as V0042_ajustada,
	case
		when V0042_ajustada is not null then 1
		else 0
	end as V0042_ajustada_1_denominador,
	case
		when V0043_ajustada is not null then V0043_ajustada
		else 0
	end as V0043_ajustada,
	case
		when V0043_ajustada is not null then 1
		else 0
	end as V0043_ajustada_1_denominador,
	case
		when V0044_ajustada is not null then V0044_ajustada
		else 0
	end as V0044_ajustada,
	case
		when V0044_ajustada is not null then 1
		else 0
	end as V0044_ajustada_1_denominador,
	case
		when V0045_ajustada is not null then V0045_ajustada
		else 0
	end as V0045_ajustada,
	case
		when V0045_ajustada is not null then 1
		else 0
	end as V0045_ajustada_1_denominador,
	case
		when V0046A_ajustada is not null then V0046A_ajustada
		else 0
	end as V0046A_ajustada,
	case
		when V0046A_ajustada is not null then 1
		else 0
	end as V0046A_ajustada_1_denominador,
	case
		when V0046B_ajustada is not null then V0046B_ajustada
		else 0
	end as V0046B_ajustada,
	case
		when V0046B_ajustada is not null then 1
		else 0
	end as V0046B_ajustada_1_denominador,
	case
		when V0047_ajustada is not null then V0047_ajustada
		else 0
	end as V0047_ajustada,
	case
		when V0047_ajustada is not null then 1
		else 0
	end as V0047_ajustada_1_denominador,
	case
		when V0048_ajustada is not null then V0048_ajustada
		else 0
	end as V0048_ajustada,
	case
		when V0048_ajustada is not null then 1
		else 0
	end as V0048_ajustada_1_denominador,
	case
		when V0049_ajustada is not null then V0049_ajustada
		else 0
	end as V0049_ajustada,
	case
		when V0049_ajustada is not null then 1
		else 0
	end as V0049_ajustada_1_denominador,
	case
		when V0050_ajustada is not null then V0050_ajustada
		else 0
	end as V0050_ajustada,
	case
		when V0050_ajustada is not null then 1
		else 0
	end as V0050_ajustada_1_denominador,
	case
		when V0051_ajustada is not null then V0051_ajustada
		else 0
	end as V0051_ajustada,
	case
		when V0051_ajustada is not null then 1
		else 0
	end as V0051_ajustada_1_denominador,
	case
		when V0052_ajustada is not null then V0052_ajustada
		else 0
	end as V0052_ajustada,
	case
		when V0052_ajustada is not null then 1
		else 0
	end as V0052_ajustada_1_denominador,
	case
		when V0054A_ajustada is not null then V0054A_ajustada
		else 0
	end as V0054A_ajustada,
	case
		when V0054A_ajustada is not null then 1
		else 0
	end as V0054A_ajustada_1_denominador,
	case
		when V0054B_ajustada is not null then V0054B_ajustada
		else 0
	end as V0054B_ajustada,
	case
		when V0054B_ajustada is not null then 1
		else 0
	end as V0054B_ajustada_1_denominador,
	case
		when V0054C_ajustada is not null then V0054C_ajustada
		else 0
	end as V0054C_ajustada,
	case
		when V0054C_ajustada is not null then 1
		else 0
	end as V0054C_ajustada_1_denominador,
	case
		when V0054D_ajustada is not null then V0054D_ajustada
		else 0
	end as V0054D_ajustada,
	case
		when V0054D_ajustada is not null then 1
		else 0
	end as V0054D_ajustada_1_denominador,
	case
		when V0055_ajustada is not null then V0055_ajustada
		else 0
	end as V0055_ajustada,
	case
		when V0055_ajustada is not null then 1
		else 0
	end as V0055_ajustada_1_denominador,
	case
		when V0055A_ajustada is not null then V0055A_ajustada
		else 0
	end as V0055A_ajustada,
	case
		when V0055A_ajustada is not null then 1
		else 0
	end as V0055A_ajustada_1_denominador,
	case
		when V0055B_ajustada is not null then V0055B_ajustada
		else 0
	end as V0055B_ajustada,
	case
		when V0055B_ajustada is not null then 1
		else 0
	end as V0055B_ajustada_1_denominador,
	case
		when V0056_ajustada is not null then V0056_ajustada
		else 0
	end as V0056_ajustada,
	case
		when V0056_ajustada is not null then 1
		else 0
	end as V0056_ajustada_1_denominador,
	case
		when V0056A_ajustada is not null then V0056A_ajustada
		else 0
	end as V0056A_ajustada,
	case
		when V0056A_ajustada is not null then 1
		else 0
	end as V0056A_ajustada_1_denominador,
	case
		when V0057A_ajustada is not null then V0057A_ajustada
		else 0
	end as V0057A_ajustada,
	case
		when V0057A_ajustada is not null then 1
		else 0
	end as V0057A_ajustada_1_denominador,
	case
		when V0057B_ajustada is not null then V0057B_ajustada
		else 0
	end as V0057B_ajustada,
	case
		when V0057B_ajustada is not null then 1
		else 0
	end as V0057B_ajustada_1_denominador,
	case
		when V0057C_ajustada is not null then V0057C_ajustada
		else 0
	end as V0057C_ajustada,
	case
		when V0057C_ajustada is not null then 1
		else 0
	end as V0057C_ajustada_1_denominador,
	case
		when V0057D_ajustada is not null then V0057D_ajustada
		else 0
	end as V0057D_ajustada,
	case
		when V0057D_ajustada is not null then 1
		else 0
	end as V0057D_ajustada_1_denominador,
	case
		when V0058_ajustada is not null then V0058_ajustada
		else 0
	end as V0058_ajustada,
	case
		when V0058_ajustada is not null then 1
		else 0
	end as V0058_ajustada_1_denominador,
	case
		when V0059_ajustada is not null then V0059_ajustada
		else 0
	end as V0059_ajustada,
	case
		when V0059_ajustada is not null then 1
		else 0
	end as V0059_ajustada_1_denominador
from
	media_por_rede)
	--------------------------------------
	-- PARTE 4 ---------
	-- CHAMAR denominadores
	--------------------------------------
,calculo_qa as (
	select
		V0101,
		--calcula QA101
case
			when (V0010_ajustada_1_denominador + V0011_ajustada_1_denominador + V0014_ajustada_1_denominador + V0020A_ajustada_1_denominador + V0020B_ajustada_1_denominador) > 0 then
        (V0010_ajustada + V0011_ajustada + V0014_ajustada + V0020A_ajustada + V0020B_ajustada) /
        (V0010_ajustada_1_denominador + V0011_ajustada_1_denominador + V0014_ajustada_1_denominador + V0020A_ajustada_1_denominador + V0020B_ajustada_1_denominador)
			else 0
		end as QA101,
		case
			when (V0010_ajustada_1_denominador + V0011_ajustada_1_denominador + V0014_ajustada_1_denominador + V0020A_ajustada_1_denominador + V0020B_ajustada_1_denominador) > 0 then
        (V0010_ajustada_1_denominador + V0011_ajustada_1_denominador + V0014_ajustada_1_denominador + V0020A_ajustada_1_denominador + V0020B_ajustada_1_denominador)
			else 0
		end as QA101_5_denominador,
		--calcula QA102
		V0010_ajustada as QA102,
		case
			when (V0010_ajustada_1_denominador) > 0 then
        (V0010_ajustada_1_denominador)
			else 0
		end as QA102_1_denominador,
		--calcula QA103
case
			when (V0011_ajustada_1_denominador + V0035_ajustada_1_denominador + V0037_ajustada_1_denominador) > 0 then
        (V0011_ajustada + V0035_ajustada + V0037_ajustada) /
        (V0011_ajustada_1_denominador + V0035_ajustada_1_denominador + V0037_ajustada_1_denominador)
			else 0
		end as QA103,
		case
			when (V0011_ajustada_1_denominador + V0035_ajustada_1_denominador + V0037_ajustada_1_denominador)> 0 then
        (V0011_ajustada_1_denominador + V0035_ajustada_1_denominador + V0037_ajustada_1_denominador)
			else 0
		end as QA103_3_denominador,
		--calcula QA13
case
			when (V0020D_ajustada_1_denominador + V0020E_ajustada_1_denominador) > 0 then
        (V0020D_ajustada + V0020E_ajustada) /
        (V0020D_ajustada_1_denominador + V0020E_ajustada_1_denominador)
			else 0
		end as QA13,
		case
			when (V0020D_ajustada_1_denominador + V0020E_ajustada_1_denominador) > 0 then
        (V0020D_ajustada_1_denominador + V0020E_ajustada_1_denominador)
			else 0
		end as QA13_2_denominador,
		--calcula QA8
case
			when (V0010_ajustada_1_denominador + V0011_ajustada_1_denominador) > 0 then
        (V0010_ajustada + V0011_ajustada) /
        (V0010_ajustada_1_denominador + V0011_ajustada_1_denominador)
			else 0
		end as QA8,
		case
			when (V0010_ajustada_1_denominador + V0011_ajustada_1_denominador) > 0 then
        (V0010_ajustada_1_denominador + V0011_ajustada_1_denominador)
			else 0
		end as QA8_2_denominador,
		--calcula QA104
case
			when (V0018_ajustada_1_denominador + V0019_ajustada_1_denominador) > 0 then
        (V0018_ajustada + V0019_ajustada) /
        (V0018_ajustada_1_denominador + V0019_ajustada_1_denominador)
			else 0
		end as QA104,
		case
			when (V0018_ajustada_1_denominador + V0019_ajustada_1_denominador) > 0 then
        (V0018_ajustada_1_denominador + V0019_ajustada_1_denominador)
			else 0
		end as QA104_2_denominador,
		--calcula QA105
case
			when (V0035_ajustada_1_denominador + V0037_ajustada_1_denominador + V0048_ajustada_1_denominador + V0051_ajustada_1_denominador) > 0 then
        (V0035_ajustada + V0037_ajustada + V0048_ajustada + V0051_ajustada) /
        (V0035_ajustada_1_denominador + V0037_ajustada_1_denominador + V0048_ajustada_1_denominador + V0051_ajustada_1_denominador)
			else 0
		end as QA105,
		case
			when (V0035_ajustada_1_denominador + V0037_ajustada_1_denominador + V0048_ajustada_1_denominador + V0051_ajustada_1_denominador) > 0 then
        (V0035_ajustada_1_denominador + V0037_ajustada_1_denominador + V0048_ajustada_1_denominador + V0051_ajustada_1_denominador)
			else 0
		end as QA105_4_denominador,
		--calcula QA106
case
			when (V0018_ajustada_1_denominador + V0019_ajustada_1_denominador + V0020D_ajustada_1_denominador + V0020E_ajustada_1_denominador) > 0 then
        (V0018_ajustada + V0019_ajustada + V0020D_ajustada + V0020E_ajustada) /
        (V0018_ajustada_1_denominador + V0019_ajustada_1_denominador + V0020D_ajustada_1_denominador + V0020E_ajustada_1_denominador)
			else 0
		end as QA106,
		case
			when (V0018_ajustada_1_denominador + V0019_ajustada_1_denominador + V0020D_ajustada_1_denominador + V0020E_ajustada_1_denominador) > 0 then
        (V0018_ajustada_1_denominador + V0019_ajustada_1_denominador + V0020D_ajustada_1_denominador + V0020E_ajustada_1_denominador)
			else 0
		end as QA106_4_denominador,
		--calcula QA16
case
			when (V0020D_ajustada_1_denominador + V0020E_ajustada_1_denominador) > 0 then
        (V0020D_ajustada + V0020E_ajustada) /
        (V0020D_ajustada_1_denominador + V0020E_ajustada_1_denominador)
			else 0
		end as QA16,
		case
			when (V0020D_ajustada_1_denominador + V0020E_ajustada_1_denominador) > 0 then
        (V0020D_ajustada_1_denominador + V0020E_ajustada_1_denominador)
			else 0
		end as QA16_2_denominador,
		--calcula QA17
case
			when (V0018_ajustada_1_denominador + V0019_ajustada_1_denominador) > 0 then
        (V0018_ajustada + V0019_ajustada) /
        (V0018_ajustada_1_denominador + V0019_ajustada_1_denominador)
			else 0
		end as QA17,
		case
			when (V0018_ajustada_1_denominador + V0019_ajustada_1_denominador) > 0 then
        (V0018_ajustada_1_denominador + V0019_ajustada_1_denominador)
			else 0
		end as QA17_2_denominador,
		--calcula QA107
case
			when (V0020D_ajustada_1_denominador + V0020E_ajustada_1_denominador + V0028B_ajustada_1_denominador + V0028C_ajustada_1_denominador + V0028D_ajustada_1_denominador) > 0 then
        (V0020D_ajustada + V0020E_ajustada + V0028B_ajustada + V0028C_ajustada + V0028D_ajustada) /
        (V0020D_ajustada_1_denominador + V0020E_ajustada_1_denominador + V0028B_ajustada_1_denominador + V0028C_ajustada_1_denominador + V0028D_ajustada_1_denominador)
			else 0
		end as QA107,
		case
			when (V0020D_ajustada_1_denominador + V0020E_ajustada_1_denominador + V0028B_ajustada_1_denominador + V0028C_ajustada_1_denominador + V0028D_ajustada_1_denominador) > 0 then
        (V0020D_ajustada_1_denominador + V0020E_ajustada_1_denominador + V0028B_ajustada_1_denominador + V0028C_ajustada_1_denominador + V0028D_ajustada_1_denominador)
			else 0
		end as QA107_5_denominador,
		--calcula QA118
		V0034_ajustada as QA118,
		case
			when (V0034_ajustada_1_denominador) > 0 then
        (V0034_ajustada_1_denominador)
			else 0
		end as QA118_1_denominador,
		--calcula QA18
case
			when (V0025A_ajustada_1_denominador + V0030A_ajustada_1_denominador + V0030B_ajustada_1_denominador + V0030C_ajustada_1_denominador) > 0 then
        (V0025A_ajustada + V0030A_ajustada + V0030B_ajustada + V0030C_ajustada) /
        (V0025A_ajustada_1_denominador + V0030A_ajustada_1_denominador + V0030B_ajustada_1_denominador + V0030C_ajustada_1_denominador)
			else 0
		end as QA18,
		case
			when (V0025A_ajustada_1_denominador + V0030A_ajustada_1_denominador + V0030B_ajustada_1_denominador + V0030C_ajustada_1_denominador) > 0 then
        (V0025A_ajustada_1_denominador + V0030A_ajustada_1_denominador + V0030B_ajustada_1_denominador + V0030C_ajustada_1_denominador)
			else 0
		end as QA18_4_denominador,
		--calcula QA20
case
			when (V0033_ajustada_1_denominador + V0035_ajustada_1_denominador + V0038_ajustada_1_denominador + V0039_ajustada_1_denominador + V0032B_ajustada_1_denominador) > 0 then
        (V0033_ajustada + V0035_ajustada + V0038_ajustada + V0039_ajustada + V0032B_ajustada) /
        (V0033_ajustada_1_denominador + V0035_ajustada_1_denominador + V0038_ajustada_1_denominador + V0039_ajustada_1_denominador + V0032B_ajustada_1_denominador)
			else 0
		end as QA20,
		case
			when (V0033_ajustada_1_denominador + V0035_ajustada_1_denominador + V0038_ajustada_1_denominador + V0039_ajustada_1_denominador + V0032B_ajustada_1_denominador) > 0 then
        (V0033_ajustada_1_denominador + V0035_ajustada_1_denominador + V0038_ajustada_1_denominador + V0039_ajustada_1_denominador + V0032B_ajustada_1_denominador)
			else 0
		end as QA20_5_denominador,
		--calcula QA21
case
			when (V0039_ajustada_1_denominador + V0038_ajustada_1_denominador + V0035_ajustada_1_denominador) > 0 then
        (V0039_ajustada + V0038_ajustada + V0035_ajustada) /
        (V0039_ajustada_1_denominador + V0038_ajustada_1_denominador + V0035_ajustada_1_denominador)
			else 0
		end as QA21,
		case
			when (V0039_ajustada_1_denominador + V0038_ajustada_1_denominador + V0035_ajustada_1_denominador) > 0 then
        (V0039_ajustada_1_denominador + V0038_ajustada_1_denominador + V0035_ajustada_1_denominador)
			else 0
		end as QA21_3_denominador,
		--calcula QA23
case
			when (V0035_ajustada_1_denominador + V0038_ajustada_1_denominador) > 0 then
        (V0035_ajustada + V0038_ajustada) /
        (V0035_ajustada_1_denominador + V0038_ajustada_1_denominador)
			else 0
		end as QA23,
		case
			when (V0035_ajustada_1_denominador + V0038_ajustada_1_denominador) > 0 then
        (V0035_ajustada_1_denominador + V0038_ajustada_1_denominador)
			else 0
		end as QA23_2_denominador,
		--calcula QA19
case
			when (V0032B_ajustada_1_denominador + V0032D_ajustada_1_denominador + V0033_ajustada_1_denominador + V0034_ajustada_1_denominador) > 0 then
        (V0032B_ajustada + V0032D_ajustada + V0033_ajustada + V0034_ajustada) /
        (V0032B_ajustada_1_denominador + V0032D_ajustada_1_denominador + V0033_ajustada_1_denominador + V0034_ajustada_1_denominador)
			else 0
		end as QA19,
		case
			when (V0032B_ajustada_1_denominador + V0032D_ajustada_1_denominador + V0033_ajustada_1_denominador + V0034_ajustada_1_denominador) > 0 then
        (V0032B_ajustada_1_denominador + V0032D_ajustada_1_denominador + V0033_ajustada_1_denominador + V0034_ajustada_1_denominador)
			else 0
		end as QA19_4_denominador,
		--calcula QA108
		V0036_ajustada as QA108,
		case
			when (V0036_ajustada_1_denominador) > 0 then
        (V0036_ajustada_1_denominador)
			else 0
		end as QA108_1_denominador,
		-- calcula QA22
case
			when (V0032A_ajustada_1_denominador + V0036_ajustada_1_denominador) > 0 then
        (V0032A_ajustada + V0036_ajustada) /
        (V0032A_ajustada_1_denominador + V0036_ajustada_1_denominador)
			else 0
		end as QA22,
		case
			when (V0032A_ajustada_1_denominador + V0036_ajustada_1_denominador) > 0 then
        (V0032A_ajustada_1_denominador + V0036_ajustada_1_denominador)
			else 0
		end as QA22_2_denominador,
		--calcula QA24
		V0023_ajustada as QA24,
		case
			when (V0023_ajustada_1_denominador) > 0 then
        (V0023_ajustada_1_denominador)
			else 0
		end as QA24_1_denominador,
		--calcula QA109
case
			when (V0010_ajustada_1_denominador + V0024_ajustada_1_denominador) > 0 then
        (V0010_ajustada + V0024_ajustada) /
        (V0010_ajustada_1_denominador + V0024_ajustada_1_denominador)
			else 0
		end as QA109,
		case
			when (V0010_ajustada_1_denominador + V0024_ajustada_1_denominador) > 0 then
        (V0010_ajustada_1_denominador + V0024_ajustada_1_denominador)
			else 0
		end as QA109_2_denominador,
		--calcula QA34
case
			when (V0028B_ajustada_1_denominador + V0028D_ajustada_1_denominador + V0028C_ajustada_1_denominador + V0024_ajustada_1_denominador) > 0 then
        (V0028B_ajustada + V0028D_ajustada + V0028C_ajustada + V0024_ajustada) /
        (V0028B_ajustada_1_denominador + V0028D_ajustada_1_denominador + V0028C_ajustada_1_denominador + V0024_ajustada_1_denominador)
			else 0
		end as QA34,
		case
			when (V0028B_ajustada_1_denominador + V0028D_ajustada_1_denominador + V0028C_ajustada_1_denominador + V0024_ajustada_1_denominador) > 0 then
        (V0028B_ajustada_1_denominador + V0028D_ajustada_1_denominador + V0028C_ajustada_1_denominador + V0024_ajustada_1_denominador)
			else 0
		end as QA34_4_denominador,
		--calcula QA25
case
			when (V0023_ajustada_1_denominador + V0013_ajustada_1_denominador) > 0 then
        (V0023_ajustada + V0013_ajustada) /
        (V0023_ajustada_1_denominador + V0013_ajustada_1_denominador)
			else 0
		end as QA25,
		case
			when (V0023_ajustada_1_denominador + V0013_ajustada_1_denominador) > 0 then
        (V0023_ajustada_1_denominador + V0013_ajustada_1_denominador)
			else 0
		end as QA25_2_denominador,
		--calcula QA32
case
			when (V0025C_ajustada_1_denominador + V0013_ajustada_1_denominador + V0020C_ajustada_1_denominador + V0025_ajustada_1_denominador) > 0 then
        (V0025C_ajustada + V0013_ajustada + V0020C_ajustada + V0025_ajustada) /
        (V0025C_ajustada_1_denominador + V0013_ajustada_1_denominador + V0020C_ajustada_1_denominador + V0025_ajustada_1_denominador)
			else 0
		end as QA32,
		case
			when (V0025C_ajustada_1_denominador + V0013_ajustada_1_denominador + V0020C_ajustada_1_denominador + V0025_ajustada_1_denominador) > 0 then
        (V0025C_ajustada_1_denominador + V0013_ajustada_1_denominador + V0020C_ajustada_1_denominador + V0025_ajustada_1_denominador)
			else 0
		end as QA32_4_denominador,
		--calcula QA29
		V0027A_ajustada as QA29,
		case
			when (V0027A_ajustada_1_denominador) > 0 then
        (V0027A_ajustada_1_denominador)
			else 0
		end as QA29_1_denominador,
		--calcula QA110
case
			when (V0027A_ajustada_1_denominador + V0026_ajustada_1_denominador + V0028A_ajustada_1_denominador + V0028B_ajustada_1_denominador + V0028C_ajustada_1_denominador + V0028D_ajustada_1_denominador) > 0 then
        (V0027A_ajustada + V0026_ajustada + V0028A_ajustada + V0028B_ajustada + V0028C_ajustada + V0028D_ajustada) /
        (V0027A_ajustada_1_denominador + V0026_ajustada_1_denominador + V0028A_ajustada_1_denominador + V0028B_ajustada_1_denominador + V0028C_ajustada_1_denominador + V0028D_ajustada_1_denominador)
			else 0
		end as QA110,
		case
			when (V0027A_ajustada_1_denominador + V0026_ajustada_1_denominador + V0028A_ajustada_1_denominador + V0028B_ajustada_1_denominador + V0028C_ajustada_1_denominador + V0028D_ajustada_1_denominador) > 0 then
        (V0027A_ajustada_1_denominador + V0026_ajustada_1_denominador + V0028A_ajustada_1_denominador + V0028B_ajustada_1_denominador + V0028C_ajustada_1_denominador + V0028D_ajustada_1_denominador)
			else 0
		end as QA110_6_denominador,
		--calcula QA31
case
			when (V0023_ajustada_1_denominador + V0026_ajustada_1_denominador + V0028A_ajustada_1_denominador + V0028B_ajustada_1_denominador + V0028C_ajustada_1_denominador + V0028D_ajustada_1_denominador + V0028E_ajustada_1_denominador + V0028F_ajustada_1_denominador + V0028G_ajustada_1_denominador) > 0 then
        (V0023_ajustada + V0026_ajustada + V0028A_ajustada + V0028B_ajustada + V0028C_ajustada + V0028D_ajustada + V0028E_ajustada + V0028F_ajustada + V0028G_ajustada) /
        (V0023_ajustada_1_denominador + V0026_ajustada_1_denominador + V0028A_ajustada_1_denominador + V0028B_ajustada_1_denominador + V0028C_ajustada_1_denominador + V0028D_ajustada_1_denominador + V0028E_ajustada_1_denominador + V0028F_ajustada_1_denominador + V0028G_ajustada_1_denominador)
			else 0
		end as QA31,
		case
			when (V0023_ajustada_1_denominador + V0026_ajustada_1_denominador + V0028A_ajustada_1_denominador + V0028B_ajustada_1_denominador + V0028C_ajustada_1_denominador + V0028D_ajustada_1_denominador + V0028E_ajustada_1_denominador + V0028F_ajustada_1_denominador + V0028G_ajustada_1_denominador) > 0 then
        (V0023_ajustada_1_denominador + V0026_ajustada_1_denominador + V0028A_ajustada_1_denominador + V0028B_ajustada_1_denominador + V0028C_ajustada_1_denominador + V0028D_ajustada_1_denominador + V0028E_ajustada_1_denominador + V0028F_ajustada_1_denominador + V0028G_ajustada_1_denominador)
			else 0
		end as QA31_9_denominador,
		--calcula QA33
case
			when (V0028A_ajustada_1_denominador + V0028B_ajustada_1_denominador + V0028C_ajustada_1_denominador + V0028D_ajustada_1_denominador) > 0 then
        (V0028A_ajustada + V0028B_ajustada + V0028C_ajustada + V0028D_ajustada) /
        (V0028A_ajustada_1_denominador + V0028B_ajustada_1_denominador + V0028C_ajustada_1_denominador + V0028D_ajustada_1_denominador)
			else 0
		end as QA33,
		case
			when (V0028A_ajustada_1_denominador + V0028B_ajustada_1_denominador + V0028C_ajustada_1_denominador + V0028D_ajustada_1_denominador) > 0 then
        (V0028A_ajustada_1_denominador + V0028B_ajustada_1_denominador + V0028C_ajustada_1_denominador + V0028D_ajustada_1_denominador)
			else 0
		end as QA33_4_denominador,
		--calcula QA35
		V0013_ajustada as QA35,
		case
			when (V0013_ajustada_1_denominador) > 0 then
        (V0013_ajustada_1_denominador)
			else 0
		end as QA35_1_denominador,
		--calcula QA36
case
			when (V0012_ajustada_1_denominador + V0020A_ajustada_1_denominador + V0020B_ajustada_1_denominador + V0041_ajustada_1_denominador + V0042_ajustada_1_denominador) > 0 then
        (V0012_ajustada + V0020A_ajustada + V0020B_ajustada + V0041_ajustada + V0042_ajustada) /
        (V0012_ajustada_1_denominador + V0020A_ajustada_1_denominador + V0020B_ajustada_1_denominador + V0041_ajustada_1_denominador + V0042_ajustada_1_denominador)
			else 0
		end as QA36,
		case
			when (V0012_ajustada_1_denominador + V0020A_ajustada_1_denominador + V0020B_ajustada_1_denominador + V0041_ajustada_1_denominador + V0042_ajustada_1_denominador) > 0 then
        (V0012_ajustada_1_denominador + V0020A_ajustada_1_denominador + V0020B_ajustada_1_denominador + V0041_ajustada_1_denominador + V0042_ajustada_1_denominador)
			else 0
		end as QA36_5_denominador,
		--calcula QA37
		V0024_ajustada as QA37,
		case
			when (V0024_ajustada_1_denominador) > 0 then
        (V0024_ajustada_1_denominador)
			else 0
		end as QA37_1_denominador,
		--calcula QA38
case
			when (V0030A_ajustada_1_denominador + V0042_ajustada_1_denominador + V0043_ajustada_1_denominador + V0044_ajustada_1_denominador + V0041_ajustada_1_denominador + V0025A_ajustada_1_denominador + V0030B_ajustada_1_denominador + V0030C_ajustada_1_denominador) > 0 then
        (V0030A_ajustada + V0042_ajustada + V0043_ajustada + V0044_ajustada + V0041_ajustada + V0025A_ajustada + V0030B_ajustada + V0030C_ajustada) /
        (V0030A_ajustada_1_denominador + V0042_ajustada_1_denominador + V0043_ajustada_1_denominador + V0044_ajustada_1_denominador + V0041_ajustada_1_denominador + V0025A_ajustada_1_denominador + V0030B_ajustada_1_denominador + V0030C_ajustada_1_denominador)
			else 0
		end as QA38,
		case
			when (V0030A_ajustada_1_denominador + V0042_ajustada_1_denominador + V0043_ajustada_1_denominador + V0044_ajustada_1_denominador + V0041_ajustada_1_denominador + V0025A_ajustada_1_denominador + V0030B_ajustada_1_denominador + V0030C_ajustada_1_denominador) > 0 then
        (V0030A_ajustada_1_denominador + V0042_ajustada_1_denominador + V0043_ajustada_1_denominador + V0044_ajustada_1_denominador + V0041_ajustada_1_denominador + V0025A_ajustada_1_denominador + V0030B_ajustada_1_denominador + V0030C_ajustada_1_denominador)
			else 0
		end as QA38_8_denominador,
		--calcula QA39
case
			when (V0020D_ajustada_1_denominador + V0020E_ajustada_1_denominador + V0044_ajustada_1_denominador) > 0 then
        (V0020D_ajustada + V0020E_ajustada + V0044_ajustada) /
        (V0020D_ajustada_1_denominador + V0020E_ajustada_1_denominador + V0044_ajustada_1_denominador)
			else 0
		end as QA39,
		--calcula QA111
case
			when (V0015A_ajustada_1_denominador + V0015B_ajustada_1_denominador + V0015C_ajustada_1_denominador + V0015D_ajustada_1_denominador + V0016_ajustada_1_denominador + V0017A_ajustada_1_denominador + V0017B_ajustada_1_denominador + V0017C_ajustada_1_denominador + V0017D_ajustada_1_denominador) > 0 then
        (V0015A_ajustada + V0015B_ajustada + V0015C_ajustada + V0015D_ajustada + V0016_ajustada + V0017A_ajustada + V0017B_ajustada + V0017C_ajustada + V0017D_ajustada) /
        (V0015A_ajustada_1_denominador + V0015B_ajustada_1_denominador + V0015C_ajustada_1_denominador + V0015D_ajustada_1_denominador + V0016_ajustada_1_denominador + V0017A_ajustada_1_denominador + V0017B_ajustada_1_denominador + V0017C_ajustada_1_denominador + V0017D_ajustada_1_denominador)
			else 0
		end as QA111,
		case
			when (V0015A_ajustada_1_denominador + V0015B_ajustada_1_denominador + V0015C_ajustada_1_denominador + V0015D_ajustada_1_denominador + V0016_ajustada_1_denominador + V0017A_ajustada_1_denominador + V0017B_ajustada_1_denominador + V0017C_ajustada_1_denominador + V0017D_ajustada_1_denominador) > 0 then
        (V0015A_ajustada_1_denominador + V0015B_ajustada_1_denominador + V0015C_ajustada_1_denominador + V0015D_ajustada_1_denominador + V0016_ajustada_1_denominador + V0017A_ajustada_1_denominador + V0017B_ajustada_1_denominador + V0017C_ajustada_1_denominador + V0017D_ajustada_1_denominador)
			else 0
		end as QA111_9_denominador,
		case
			when (V0020D_ajustada_1_denominador + V0020E_ajustada_1_denominador + V0044_ajustada_1_denominador) > 0 then
        (V0020D_ajustada_1_denominador + V0020E_ajustada_1_denominador + V0044_ajustada_1_denominador)
			else 0
		end as QA39_3_denominador,
		--calcula QA112
case
			when (V0025A_ajustada_1_denominador + V0030A_ajustada_1_denominador + V0030B_ajustada_1_denominador + V0030C_ajustada_1_denominador + V0031_ajustada_1_denominador + V0031B_ajustada_1_denominador + V0032D_ajustada_1_denominador + V0033_ajustada_1_denominador + V0032A_ajustada_1_denominador + V0032B_ajustada_1_denominador) > 0 then
        (V0025A_ajustada + V0030A_ajustada + V0030B_ajustada + V0030C_ajustada + V0031_ajustada + V0031B_ajustada + V0032D_ajustada + V0033_ajustada + V0032A_ajustada + V0032B_ajustada) /
        (V0025A_ajustada_1_denominador + V0030A_ajustada_1_denominador + V0030B_ajustada_1_denominador + V0030C_ajustada_1_denominador + V0031_ajustada_1_denominador + V0031B_ajustada_1_denominador + V0032D_ajustada_1_denominador + V0033_ajustada_1_denominador + V0032A_ajustada_1_denominador + V0032B_ajustada_1_denominador)
			else 0
		end as QA112,
		case
			when (V0025A_ajustada_1_denominador + V0030A_ajustada_1_denominador + V0030B_ajustada_1_denominador + V0030C_ajustada_1_denominador + V0031_ajustada_1_denominador + V0031B_ajustada_1_denominador + V0032D_ajustada_1_denominador + V0033_ajustada_1_denominador + V0032A_ajustada_1_denominador + V0032B_ajustada_1_denominador) > 0 then
        (V0025A_ajustada_1_denominador + V0030A_ajustada_1_denominador + V0030B_ajustada_1_denominador + V0030C_ajustada_1_denominador + V0031_ajustada_1_denominador + V0031B_ajustada_1_denominador + V0032D_ajustada_1_denominador + V0033_ajustada_1_denominador + V0032A_ajustada_1_denominador + V0032B_ajustada_1_denominador)
			else 0
		end as QA112_11_denominador,
		--calcula QA40
case
			when (V0041_ajustada_1_denominador + V0046A_ajustada_1_denominador + V0048_ajustada_1_denominador + V0049_ajustada_1_denominador + V0051_ajustada_1_denominador) > 0 then
        (V0041_ajustada + V0046A_ajustada + V0048_ajustada + V0049_ajustada + V0051_ajustada) /
        (V0041_ajustada_1_denominador + V0046A_ajustada_1_denominador + V0048_ajustada_1_denominador + V0049_ajustada_1_denominador + V0051_ajustada_1_denominador)
			else 0
		end as QA40,
		case
			when (V0041_ajustada_1_denominador + V0046A_ajustada_1_denominador + V0048_ajustada_1_denominador + V0049_ajustada_1_denominador + V0051_ajustada_1_denominador) > 0 then
        (V0041_ajustada_1_denominador + V0046A_ajustada_1_denominador + V0048_ajustada_1_denominador + V0049_ajustada_1_denominador + V0051_ajustada_1_denominador)
			else 0
		end as QA40_5_denominador,
		--calcula QA42
case
			when (V0048_ajustada_1_denominador + V0051_ajustada_1_denominador + V0049_ajustada_1_denominador + V0050_ajustada_1_denominador + V0047_ajustada_1_denominador + V0046B_ajustada_1_denominador) > 0 then
        (V0048_ajustada + V0051_ajustada + V0049_ajustada + V0050_ajustada + V0047_ajustada + V0046B_ajustada) /
        (V0048_ajustada_1_denominador + V0051_ajustada_1_denominador + V0049_ajustada_1_denominador + V0050_ajustada_1_denominador + V0047_ajustada_1_denominador + V0046B_ajustada_1_denominador)
			else 0
		end as QA42,
		case
			when (V0048_ajustada_1_denominador + V0051_ajustada_1_denominador + V0049_ajustada_1_denominador + V0050_ajustada_1_denominador + V0047_ajustada_1_denominador + V0046B_ajustada_1_denominador) > 0 then
        (V0048_ajustada_1_denominador + V0051_ajustada_1_denominador + V0049_ajustada_1_denominador + V0050_ajustada_1_denominador + V0047_ajustada_1_denominador + V0046B_ajustada_1_denominador)
			else 0
		end as QA42_6_denominador,
		--calcula QA43
case
			when (V0046B_ajustada_1_denominador + V0047_ajustada_1_denominador + V0050_ajustada_1_denominador) > 0 then
        (V0046B_ajustada + V0047_ajustada + V0050_ajustada) /
        (V0046B_ajustada_1_denominador + V0047_ajustada_1_denominador + V0050_ajustada_1_denominador)
			else 0
		end as QA43,
		case
			when (V0046B_ajustada_1_denominador + V0047_ajustada_1_denominador + V0050_ajustada_1_denominador) > 0 then
        (V0046B_ajustada_1_denominador + V0047_ajustada_1_denominador + V0050_ajustada_1_denominador)
			else 0
		end as QA43_3_denominador,
		--calcula QA44
case
			when (V0046B_ajustada_1_denominador + V0047_ajustada_1_denominador + V0050_ajustada_1_denominador) > 0 then
        (V0046B_ajustada + V0047_ajustada + V0050_ajustada) /
        (V0046B_ajustada_1_denominador + V0047_ajustada_1_denominador + V0050_ajustada_1_denominador)
			else 0
		end as QA44,
		case
			when (V0046B_ajustada_1_denominador + V0047_ajustada_1_denominador + V0050_ajustada_1_denominador) > 0 then
        (V0046B_ajustada_1_denominador + V0047_ajustada_1_denominador + V0050_ajustada_1_denominador)
			else 0
		end as QA44_3_denominador,
		--calcula QA50
case
			when (V0048_ajustada_1_denominador + V0046A_ajustada_1_denominador) > 0 then
        (V0048_ajustada + V0046A_ajustada) /
        (V0048_ajustada_1_denominador + V0046A_ajustada_1_denominador)
			else 0
		end as QA50,
		case
			when (V0048_ajustada_1_denominador + V0046A_ajustada_1_denominador) > 0 then
        (V0048_ajustada_1_denominador + V0046A_ajustada_1_denominador)
			else 0
		end as QA50_2_denominador,
		--calcula QA113
		V0052_ajustada as QA113,
		case
			when (V0052_ajustada_1_denominador) > 0 then
        (V0052_ajustada_1_denominador)
			else 0
		end as QA113_1_denominador,
		--calcula QA114
		V0052_ajustada as QA114,
		case
			when (V0052_ajustada_1_denominador) > 0 then
        (V0052_ajustada_1_denominador)
			else 0
		end as QA114_1_denominador,
		--calcula QA51
		V0052_ajustada as QA51,
		case
			when (V0052_ajustada_1_denominador) > 0 then
        (V0052_ajustada_1_denominador)
			else 0
		end as QA51_1_denominador,
		--calcula QA52
case
			when (V0041_ajustada_1_denominador + V0045_ajustada_1_denominador + V0054A_ajustada_1_denominador + V0054B_ajustada_1_denominador + V0054C_ajustada_1_denominador + V0054D_ajustada_1_denominador) > 0 then
        (V0041_ajustada + V0045_ajustada + V0054A_ajustada + V0054B_ajustada + V0054C_ajustada + V0054D_ajustada) /
        (V0041_ajustada_1_denominador + V0045_ajustada_1_denominador + V0054A_ajustada_1_denominador + V0054B_ajustada_1_denominador + V0054C_ajustada_1_denominador + V0054D_ajustada_1_denominador)
			else 0
		end as QA52,
		case
			when (V0041_ajustada_1_denominador + V0045_ajustada_1_denominador + V0054A_ajustada_1_denominador + V0054B_ajustada_1_denominador + V0054C_ajustada_1_denominador + V0054D_ajustada_1_denominador) > 0 then
        (V0041_ajustada_1_denominador + V0045_ajustada_1_denominador + V0054A_ajustada_1_denominador + V0054B_ajustada_1_denominador + V0054C_ajustada_1_denominador + V0054D_ajustada_1_denominador)
			else 0
		end as QA52_6_denominador,
		--calcula QA53
case
			when (V0054A_ajustada_1_denominador + V0054B_ajustada_1_denominador + V0054C_ajustada_1_denominador + V0054D_ajustada_1_denominador) > 0 then
        (V0054A_ajustada + V0054B_ajustada + V0054C_ajustada + V0054D_ajustada) /
        (V0054A_ajustada_1_denominador + V0054B_ajustada_1_denominador + V0054C_ajustada_1_denominador + V0054D_ajustada_1_denominador)
			else 0
		end as QA53,
		case
			when (V0054A_ajustada_1_denominador + V0054B_ajustada_1_denominador + V0054C_ajustada_1_denominador + V0054D_ajustada_1_denominador) > 0 then
        (V0054A_ajustada_1_denominador + V0054B_ajustada_1_denominador + V0054C_ajustada_1_denominador + V0054D_ajustada_1_denominador)
			else 0
		end as QA53_4_denominador,
		--calcula QA115
case
			when (V0043_ajustada_1_denominador + V0055A_ajustada_1_denominador + V0055B_ajustada_1_denominador) > 0 then
        (V0043_ajustada + V0055A_ajustada + V0055B_ajustada) /
        (V0043_ajustada_1_denominador + V0055A_ajustada_1_denominador + V0055B_ajustada_1_denominador)
			else 0
		end as QA115,
		case
			when (V0043_ajustada_1_denominador + V0055A_ajustada_1_denominador + V0055B_ajustada_1_denominador) > 0 then
        (V0043_ajustada_1_denominador + V0055A_ajustada_1_denominador + V0055B_ajustada_1_denominador)
			else 0
		end as QA115_3_denominador,
		--calcula QA66
case
			when (V0056_ajustada_1_denominador + V0056A_ajustada_1_denominador) > 0 then
        (V0056_ajustada + V0056A_ajustada) /
        (V0056_ajustada_1_denominador + V0056A_ajustada_1_denominador)
			else 0
		end as QA66,
		case
			when (V0056_ajustada_1_denominador + V0056A_ajustada_1_denominador) > 0 then
        (V0056_ajustada_1_denominador + V0056A_ajustada_1_denominador)
			else 0
		end as QA66_2_denominador,
		--calcula QA58
case
			when (V0057A_ajustada_1_denominador + V0057B_ajustada_1_denominador + V0057C_ajustada_1_denominador + V0057D_ajustada_1_denominador + V0059_ajustada_1_denominador) > 0 then
        (V0057A_ajustada + V0057B_ajustada + V0057C_ajustada + V0057D_ajustada + V0059_ajustada) /
        (V0057A_ajustada_1_denominador + V0057B_ajustada_1_denominador + V0057C_ajustada_1_denominador + V0057D_ajustada_1_denominador + V0059_ajustada_1_denominador)
			else 0
		end as QA58,
		case
			when (V0057A_ajustada_1_denominador + V0057B_ajustada_1_denominador + V0057C_ajustada_1_denominador + V0057D_ajustada_1_denominador + V0059_ajustada_1_denominador) > 0 then
        (V0057A_ajustada_1_denominador + V0057B_ajustada_1_denominador + V0057C_ajustada_1_denominador + V0057D_ajustada_1_denominador + V0059_ajustada_1_denominador)
			else 0
		end as QA58_5_denominador,
		--calcula QA59
case
			when (V0057A_ajustada_1_denominador + V0057B_ajustada_1_denominador + V0057C_ajustada_1_denominador + V0057D_ajustada_1_denominador + V0059_ajustada_1_denominador) > 0 then
        (V0057A_ajustada + V0057B_ajustada + V0057C_ajustada + V0057D_ajustada + V0059_ajustada) /
        (V0057A_ajustada_1_denominador + V0057B_ajustada_1_denominador + V0057C_ajustada_1_denominador + V0057D_ajustada_1_denominador + V0059_ajustada_1_denominador)
			else 0
		end as QA59,
		case
			when (V0057A_ajustada_1_denominador + V0057B_ajustada_1_denominador + V0057C_ajustada_1_denominador + V0057D_ajustada_1_denominador + V0059_ajustada_1_denominador) > 0 then
        (V0057A_ajustada_1_denominador + V0057B_ajustada_1_denominador + V0057C_ajustada_1_denominador + V0057D_ajustada_1_denominador + V0059_ajustada_1_denominador)
			else 0
		end as QA59_5_denominador,
		--calcula QA63
case
			when (V0058_ajustada_1_denominador + V0055_ajustada_1_denominador) > 0 then
        (V0058_ajustada + V0055_ajustada) /
        (V0058_ajustada_1_denominador + V0055_ajustada_1_denominador)
			else 0
		end as QA63,
		case
			when (V0058_ajustada_1_denominador + V0055_ajustada_1_denominador) > 0 then
        (V0058_ajustada_1_denominador + V0055_ajustada_1_denominador)
			else 0
		end as QA63_2_denominador,
		--calcula QA116
case
			when (V0015A_ajustada_1_denominador + V0015B_ajustada_1_denominador + V0015C_ajustada_1_denominador + V0015D_ajustada_1_denominador + V0016_ajustada_1_denominador + V0017A_ajustada_1_denominador + V0017B_ajustada_1_denominador + V0017C_ajustada_1_denominador + V0017D_ajustada_1_denominador) > 0 then
        (V0015A_ajustada + V0015B_ajustada + V0015C_ajustada + V0015D_ajustada + V0016_ajustada + V0017A_ajustada + V0017B_ajustada + V0017C_ajustada + V0017D_ajustada) /
        (V0015A_ajustada_1_denominador + V0015B_ajustada_1_denominador + V0015C_ajustada_1_denominador + V0015D_ajustada_1_denominador + V0016_ajustada_1_denominador + V0017A_ajustada_1_denominador + V0017B_ajustada_1_denominador + V0017C_ajustada_1_denominador + V0017D_ajustada_1_denominador)
			else 0
		end as QA116,
		case
			when (V0015A_ajustada_1_denominador + V0015B_ajustada_1_denominador + V0015C_ajustada_1_denominador + V0015D_ajustada_1_denominador + V0016_ajustada_1_denominador + V0017A_ajustada_1_denominador + V0017B_ajustada_1_denominador + V0017C_ajustada_1_denominador + V0017D_ajustada_1_denominador) > 0 then
        (V0015A_ajustada_1_denominador + V0015B_ajustada_1_denominador + V0015C_ajustada_1_denominador + V0015D_ajustada_1_denominador + V0016_ajustada_1_denominador + V0017A_ajustada_1_denominador + V0017B_ajustada_1_denominador + V0017C_ajustada_1_denominador + V0017D_ajustada_1_denominador)
			else 0
		end as QA116_9_denominador,
		--calcula QA117
case
			when (V0028A_ajustada_1_denominador + V0028B_ajustada_1_denominador + V0028C_ajustada_1_denominador + V0028D_ajustada_1_denominador + V0028E_ajustada_1_denominador + V0028F_ajustada_1_denominador + V0028G_ajustada_1_denominador) > 0 then
        (V0028A_ajustada + V0028B_ajustada + V0028C_ajustada + V0028D_ajustada + V0028E_ajustada + V0028F_ajustada + V0028G_ajustada) /
        (V0028A_ajustada_1_denominador + V0028B_ajustada_1_denominador + V0028C_ajustada_1_denominador + V0028D_ajustada_1_denominador + V0028E_ajustada_1_denominador + V0028F_ajustada_1_denominador + V0028G_ajustada_1_denominador)
			else 0
		end as QA117,
		case
			when (V0028A_ajustada_1_denominador + V0028B_ajustada_1_denominador + V0028C_ajustada_1_denominador + V0028D_ajustada_1_denominador + V0028E_ajustada_1_denominador + V0028F_ajustada_1_denominador + V0028G_ajustada_1_denominador) > 0 then
        (V0028A_ajustada_1_denominador + V0028B_ajustada_1_denominador + V0028C_ajustada_1_denominador + V0028D_ajustada_1_denominador + V0028E_ajustada_1_denominador + V0028F_ajustada_1_denominador + V0028G_ajustada_1_denominador)
			else 0
		end as QA117_7_denominador,
		--calcula QA65
case
			when (V0054C_ajustada_1_denominador + V0057D_ajustada_1_denominador) > 0 then
        (V0054C_ajustada + V0057D_ajustada) /
        (V0054C_ajustada_1_denominador + V0057D_ajustada_1_denominador)
			else 0
		end as QA65,
		case
			when (V0054C_ajustada_1_denominador + V0057D_ajustada_1_denominador) > 0 then
        (V0054C_ajustada_1_denominador + V0057D_ajustada_1_denominador)
			else 0
		end as QA65_2_denominador
	from
		denominadores)
	--------------------------------------
	-- PARTE 5 ---------
	-- CHAMAR calculo_qa
	--------------------------------------
, denominadores_qa as (
select
	V0101,
	QA101,
	case
		when QA101_5_denominador > 0 then 1
		else 0
	end as QA101_1_denominador,
	QA102,
	case
		when QA102_1_denominador > 0 then 1
		else 0
	end as QA102_1_denominador,
	QA103,
	case
		when QA103_3_denominador > 0 then 1
		else 0
	end as QA103_1_denominador,
	QA13,
	case
		when QA13_2_denominador > 0 then 1
		else 0
	end as QA13_1_denominador,
	QA8,
	case
		when QA8_2_denominador > 0 then 1
		else 0
	end as QA8_1_denominador,
	QA104,
	case
		when QA104_2_denominador > 0 then 1
		else 0
	end as QA104_1_denominador,
	QA105,
	case
		when QA105_4_denominador > 0 then 1
		else 0
	end as QA105_1_denominador,
	QA106,
	case
		when QA106_4_denominador > 0 then 1
		else 0
	end as QA106_1_denominador,
	QA16,
	case
		when QA16_2_denominador > 0 then 1
		else 0
	end as QA16_1_denominador,
	QA17,
	case
		when QA17_2_denominador > 0 then 1
		else 0
	end as QA17_1_denominador,
	QA107,
	case
		when QA107_5_denominador > 0 then 1
		else 0
	end as QA107_1_denominador,
	QA118,
	case
		when QA118_1_denominador > 0 then 1
		else 0
	end as QA118_1_denominador,
	QA18,
	case
		when QA18_4_denominador > 0 then 1
		else 0
	end as QA18_1_denominador,
	QA20,
	case
		when QA20_5_denominador > 0 then 1
		else 0
	end as QA20_1_denominador,
	QA21,
	case
		when QA21_3_denominador > 0 then 1
		else 0
	end as QA21_1_denominador,
	QA23,
	case
		when QA23_2_denominador > 0 then 1
		else 0
	end as QA23_1_denominador,
	QA19,
	case
		when QA19_4_denominador > 0 then 1
		else 0
	end as QA19_1_denominador,
	QA108,
	case
		when QA108_1_denominador > 0 then 1
		else 0
	end as QA108_1_denominador,
	QA22,
	case
		when QA22_2_denominador > 0 then 1
		else 0
	end as QA22_1_denominador,
	QA24,
	case
		when QA24_1_denominador > 0 then 1
		else 0
	end as QA24_1_denominador,
	QA109,
	case
		when QA109_2_denominador > 0 then 1
		else 0
	end as QA109_1_denominador,
	QA34,
	case
		when QA34_4_denominador > 0 then 1
		else 0
	end as QA34_1_denominador,
	QA25,
	case
		when QA25_2_denominador > 0 then 1
		else 0
	end as QA25_1_denominador,
	QA32,
	case
		when QA32_4_denominador > 0 then 1
		else 0
	end as QA32_1_denominador,
	QA29,
	case
		when QA29_1_denominador > 0 then 1
		else 0
	end as QA29_1_denominador,
	QA110,
	case
		when QA110_6_denominador > 0 then 1
		else 0
	end as QA110_1_denominador,
	QA31,
	case
		when QA31_9_denominador > 0 then 1
		else 0
	end as QA31_1_denominador,
	QA33,
	case
		when QA33_4_denominador > 0 then 1
		else 0
	end as QA33_1_denominador,
	QA35,
	case
		when QA35_1_denominador > 0 then 1
		else 0
	end as QA35_1_denominador,
	QA36,
	case
		when QA36_5_denominador > 0 then 1
		else 0
	end as QA36_1_denominador,
	QA37,
	case
		when QA37_1_denominador > 0 then 1
		else 0
	end as QA37_1_denominador,
	QA38,
	case
		when QA38_8_denominador > 0 then 1
		else 0
	end as QA38_1_denominador,
	QA39,
	case
		when QA39_3_denominador > 0 then 1
		else 0
	end as QA39_1_denominador,
	QA111,
	case
		when QA111_9_denominador > 0 then 1
		else 0
	end as QA111_1_denominador,
	QA112,
	case
		when QA112_11_denominador > 0 then 1
		else 0
	end as QA112_1_denominador,
	QA40,
	case
		when QA40_5_denominador > 0 then 1
		else 0
	end as QA40_1_denominador,
	QA42,
	case
		when QA42_6_denominador > 0 then 1
		else 0
	end as QA42_1_denominador,
	QA43,
	case
		when QA43_3_denominador > 0 then 1
		else 0
	end as QA43_1_denominador,
	QA44,
	case
		when QA44_3_denominador > 0 then 1
		else 0
	end as QA44_1_denominador,
	QA50,
	case
		when QA50_2_denominador > 0 then 1
		else 0
	end as QA50_1_denominador,
	QA113,
	case
		when QA113_1_denominador > 0 then 1
		else 0
	end as QA113_1_denominador ,
	QA114,
	case
		when QA114_1_denominador > 0 then 1
		else 0
	end as QA114_1_denominador,
	QA51,
	case
		when QA51_1_denominador > 0 then 1
		else 0
	end as QA51_1_denominador,
	QA52,
	case
		when QA52_6_denominador > 0 then 1
		else 0
	end as QA52_1_denominador,
	QA53,
	case
		when QA53_4_denominador > 0 then 1
		else 0
	end as QA53_1_denominador,
	QA115,
	case
		when QA115_3_denominador > 0 then 1
		else 0
	end as QA115_1_denominador,
	QA66,
	case
		when QA66_2_denominador > 0 then 1
		else 0
	end as QA66_1_denominador,
	QA58,
	case
		when QA58_5_denominador > 0 then 1
		else 0
	end as QA58_1_denominador,
	QA59,
	case
		when QA59_5_denominador > 0 then 1
		else 0
	end as QA59_1_denominador,
	QA63,
	case
		when QA63_2_denominador > 0 then 1
		else 0
	end as QA63_1_denominador,
	QA116,
	case
		when QA116_9_denominador > 0 then 1
		else 0
	end as QA116_1_denominador,
	QA117,
	case
		when QA117_7_denominador > 0 then 1
		else 0
	end as QA117_1_denominador,
	QA65,
	case
		when QA65_2_denominador > 0 then 1
		else 0
	end as QA65_1_denominador
from
	calculo_qa
order by
	V0101 asc
	)
	--------------------------------------
	-- PARTE 6 ---------
	-- CHAMAR denominadores_qa
	--------------------------------------	
, calculo_atributos as (select
	V0101,
	--calcula infraestrutura
case
		when (QA101_1_denominador + QA102_1_denominador + QA103_1_denominador + QA13_1_denominador) > 0 then
        (QA101 + QA102 + QA103 + QA13) /
        (QA101_1_denominador + QA102_1_denominador + QA103_1_denominador + QA13_1_denominador)
		else 0
	end as infraestrutura,
	case
		when (QA101_1_denominador + QA102_1_denominador + QA103_1_denominador + QA13_1_denominador) > 0 then
        (QA101_1_denominador + QA102_1_denominador + QA103_1_denominador + QA13_1_denominador)
		else 0
	end as infraestrutura_4_denominador,
	--calcula governanca
case
		when (QA104_1_denominador + QA8_1_denominador + QA105_1_denominador + QA106_1_denominador + QA16_1_denominador + QA17_1_denominador) > 0 then
        (QA104 + QA8 + QA105 + QA106 + QA16 + QA17) /
        (QA104_1_denominador + QA8_1_denominador + QA105_1_denominador + QA106_1_denominador + QA16_1_denominador + QA17_1_denominador)
		else 0
	end as governanca,
	case
		when (QA104_1_denominador + QA8_1_denominador + QA105_1_denominador + QA106_1_denominador + QA16_1_denominador + QA17_1_denominador) > 0 then
        (QA104_1_denominador + QA8_1_denominador + QA105_1_denominador + QA106_1_denominador + QA16_1_denominador + QA17_1_denominador)
		else 0
	end as governanca_6_denominador,
	--calcula propriedades
case
		when (QA107_1_denominador + QA118_1_denominador) > 0 then
        (QA107 + QA118) /
        (QA107_1_denominador + QA118_1_denominador)
		else 0
	end as propriedades,
	case
		when (QA107_1_denominador + QA118_1_denominador) > 0 then
        (QA107_1_denominador + QA118_1_denominador)
		else 0
	end as propriedades_2_denominador,
	--calcula qualidade_padronizacao
case
		when (QA18_1_denominador + QA20_1_denominador + QA21_1_denominador + QA23_1_denominador) > 0 then
        (QA18 + QA20 + QA21 + QA23) /
        (QA18_1_denominador + QA20_1_denominador + QA21_1_denominador + QA23_1_denominador)
		else 0
	end as qualidade_padronizacao,
	case
		when (QA18_1_denominador + QA20_1_denominador + QA21_1_denominador + QA23_1_denominador) > 0 then
        (QA18_1_denominador + QA20_1_denominador + QA21_1_denominador + QA23_1_denominador)
		else 0
	end as qualidade_padronizacao_4_denominador,
	--calcula tecnologia_metodos
case
		when (QA19_1_denominador) > 0 then
        (QA19) /
        (QA19_1_denominador)
		else 0
	end as tecnologia_metodos,
	case
		when (QA19_1_denominador) > 0 then
        (QA19_1_denominador)
		else 0
	end as tecnologia_metodos_1_denominador,
	--calcula habilidades_conhecimentos_a
case
		when (QA108_1_denominador + QA22_1_denominador) > 0 then
        (QA108 + QA22) /
        (QA108_1_denominador + QA22_1_denominador)
		else 0
	end as habilidades_conhecimentos_a,
	case
		when (QA108_1_denominador + QA22_1_denominador) > 0 then
        (QA108_1_denominador + QA22_1_denominador)
		else 0
	end as habilidades_conhecimentos_a_2_denominador,
	--calcula aplicativos_ferramentas
case
		when (QA24_1_denominador + QA109_1_denominador + QA34_1_denominador) > 0 then
        (QA24 + QA109 + QA34) /
        (QA24_1_denominador + QA109_1_denominador + QA34_1_denominador)
		else 0
	end as aplicativos_ferramentas,
	case
		when (QA24_1_denominador + QA109_1_denominador + QA34_1_denominador) > 0 then
        (QA24_1_denominador + QA109_1_denominador + QA34_1_denominador)
		else 0
	end as aplicativos_ferramentas_3_denominador,
	--calcula tecnicas_analises
case
		when (QA25_1_denominador + QA32_1_denominador) > 0 then
        (QA25 + QA32) /
        (QA25_1_denominador + QA32_1_denominador)
		else 0
	end as tecnicas_analises,
	case
		when (QA25_1_denominador + QA32_1_denominador) > 0 then
        (QA25_1_denominador + QA32_1_denominador)
		else 0
	end as tecnicas_analises_2_denominador,
	--calcula habilidades_conhecimentos_b
case
		when (QA29_1_denominador + QA31_1_denominador + QA110_1_denominador + QA33_1_denominador) > 0 then
        (QA29 + QA31 + QA110 + QA33) /
        (QA29_1_denominador + QA31_1_denominador + QA110_1_denominador + QA33_1_denominador)
		else 0
	end as habilidades_conhecimentos_b,
	case
		when (QA29_1_denominador + QA31_1_denominador + QA110_1_denominador + QA33_1_denominador) > 0 then
        (QA29_1_denominador + QA31_1_denominador + QA110_1_denominador + QA33_1_denominador)
		else 0
	end as habilidades_conhecimentos_b_4_denominador,
	--calcula requisitos_uso
case
		when (QA35_1_denominador + QA36_1_denominador + QA37_1_denominador + QA38_1_denominador + QA39_1_denominador + QA112_1_denominador) > 0 then
        (QA35 + QA36 + QA37 + QA38 + QA39 + QA111 + QA112) /
        (QA35_1_denominador + QA36_1_denominador + QA37_1_denominador + QA38_1_denominador + QA39_1_denominador + QA111_1_denominador + QA112_1_denominador)
		else 0
	end as requisitos_uso,
	case
		when (QA35_1_denominador + QA36_1_denominador + QA37_1_denominador + QA38_1_denominador + QA39_1_denominador + QA111_1_denominador + QA112_1_denominador) > 0 then
        (QA35_1_denominador + QA36_1_denominador + QA37_1_denominador + QA38_1_denominador + QA39_1_denominador + QA111_1_denominador + QA112_1_denominador)
		else 0
	end as requisitos_uso_6_denominador,
	--calcula gestao_conhecimento
case
		when (QA40_1_denominador + QA42_1_denominador + QA43_1_denominador + QA44_1_denominador + QA50_1_denominador) > 0 then
        (QA40 + QA42 + QA43 + QA44 + QA50) /
        (QA40_1_denominador + QA42_1_denominador + QA43_1_denominador + QA44_1_denominador + QA50_1_denominador)
		else 0
	end as gestao_conhecimento,
	case
		when (QA40_1_denominador + QA42_1_denominador + QA43_1_denominador + QA44_1_denominador + QA50_1_denominador) > 0 then
        (QA40_1_denominador + QA42_1_denominador + QA43_1_denominador + QA44_1_denominador + QA50_1_denominador)
		else 0
	end as gestao_conhecimento_5_denominador,
	--calcula governanca_informacao
case
		when (QA113_1_denominador + QA114_1_denominador + QA51_1_denominador) > 0 then
        (QA113 + QA114 + QA51) /
        (QA113_1_denominador + QA114_1_denominador + QA51_1_denominador)
		else 0
	end as governanca_informacao,
	case
		when (QA113_1_denominador + QA114_1_denominador + QA51_1_denominador) > 0 then
        (QA113_1_denominador + QA114_1_denominador + QA51_1_denominador)
		else 0
	end as governanca_informacao_3_denominador,
	--calcula metas_resultados
case
		when (QA52_1_denominador + QA53_1_denominador + QA115_1_denominador + QA66_1_denominador) > 0 then
        (QA52 + QA53 + QA115 + QA66) /
        (QA52_1_denominador + QA53_1_denominador + QA115_1_denominador + QA66_1_denominador)
		else 0
	end as metas_resultados,
	case
		when (QA52_1_denominador + QA53_1_denominador + QA115_1_denominador + QA66_1_denominador) > 0 then
        (QA52_1_denominador + QA53_1_denominador + QA115_1_denominador + QA66_1_denominador)
		else 0
	end as metas_resultados_4_denominador,
	--calcula processo_tomada_decisao
case
		when (QA58_1_denominador + QA59_1_denominador + QA63_1_denominador) > 0 then
        (QA58 + QA59 + QA63) /
        (QA58_1_denominador + QA59_1_denominador + QA63_1_denominador)
		else 0
	end as processo_tomada_decisao,
	case
		when (QA58_1_denominador + QA59_1_denominador + QA63_1_denominador) > 0 then
        (QA58_1_denominador + QA59_1_denominador + QA63_1_denominador)
		else 0
	end as processo_tomada_decisao_3_denominador,
	--calcula lideranca_empoderamento
case
		when (QA116_1_denominador + QA117_1_denominador + QA65_1_denominador) > 0 then
        (QA116 + QA117 + QA65) /
        (QA116_1_denominador + QA117_1_denominador + QA65_1_denominador)
		else 0
	end as lideranca_empoderamento,
	case
		when (QA116_1_denominador + QA117_1_denominador + QA65_1_denominador) > 0 then
        (QA116_1_denominador + QA117_1_denominador + QA65_1_denominador)
		else 0
	end as lideranca_empoderamento_3_denominador
from
	denominadores_qa)	
	--------------------------------------
	-- PARTE 7 ---------
	-- CHAMAR calculo_atributos
	--------------------------------------
, denominadores_atributos as (
select
	V0101,
	infraestrutura,
	case
		when infraestrutura_4_denominador > 0 then 1
		else 0
	end as infraestrutura_1_denominador,
	governanca,
	case
		when governanca_6_denominador > 0 then 1
		else 0
	end as governanca_1_denominador,
	propriedades,
	case
		when propriedades_2_denominador > 0 then 1
		else 0
	end as propriedades_1_denominador,
	qualidade_padronizacao,
	case
		when qualidade_padronizacao_4_denominador > 0 then 1
		else 0
	end as qualidade_padronizacao_1_denominador,
	tecnologia_metodos,
	case
		when tecnologia_metodos_1_denominador > 0 then 1
		else 0
	end as tecnologia_metodos_1_denominador,
	habilidades_conhecimentos_a,
	case
		when habilidades_conhecimentos_a_2_denominador > 0 then 1
		else 0
	end as habilidades_conhecimentos_a_1_denominador,
	aplicativos_ferramentas,
	case
		when aplicativos_ferramentas_3_denominador > 0 then 1
		else 0
	end as aplicativos_ferramentas_1_denominador,
	tecnicas_analises,
	case
		when tecnicas_analises_2_denominador > 0 then 1
		else 0
	end as tecnicas_analises_1_denominador,
	habilidades_conhecimentos_b,
	case
		when habilidades_conhecimentos_b_4_denominador > 0 then 1
		else 0
	end as habilidades_conhecimentos_b_1_denominador,
	requisitos_uso,
	case
		when requisitos_uso_6_denominador > 0 then 1
		else 0
	end as requisitos_uso_1_denominador,
	gestao_conhecimento,
	case
		when gestao_conhecimento_5_denominador > 0 then 1
		else 0
	end as gestao_conhecimento_1_denominador,
	governanca_informacao,
	case
		when governanca_informacao_3_denominador > 0 then 1
		else 0
	end as governanca_informacao_1_denominador,
	metas_resultados,
	case
		when metas_resultados_4_denominador > 0 then 1
		else 0
	end as metas_resultados_1_denominador,
	processo_tomada_decisao,
	case
		when processo_tomada_decisao_3_denominador > 0 then 1
		else 0
	end as processo_tomada_decisao_1_denominador,
	lideranca_empoderamento,
	case
		when lideranca_empoderamento_3_denominador > 0 then 1
		else 0
	end as lideranca_empoderamento_1_denominador
from calculo_atributos) 
	--------------------------------------
	-- PARTE 8 ---------
	-- CHAMAR denominadores_atributos
	--------------------------------------
, calculo_dimensoes as (
select
	V0101,
	--calcula disponibilidade_dados
case
		when (infraestrutura_1_denominador + governanca_1_denominador + propriedades_1_denominador) > 0 then
        (infraestrutura + governanca + propriedades) /
        (infraestrutura_1_denominador + governanca_1_denominador + propriedades_1_denominador)
		else 0
	end as disponibilidade_dados,
	case
		when (infraestrutura_1_denominador + governanca_1_denominador + propriedades_1_denominador) > 0 then
        (infraestrutura_1_denominador + governanca_1_denominador + propriedades_1_denominador)
		else 0
	end as disponibilidade_dados_3_denominador,
	--calcula qualidade_dados
case
		when (qualidade_padronizacao_1_denominador + tecnologia_metodos_1_denominador + habilidades_conhecimentos_a_1_denominador) > 0 then
        (qualidade_padronizacao + tecnologia_metodos + habilidades_conhecimentos_a) /
        (qualidade_padronizacao_1_denominador + tecnologia_metodos_1_denominador + habilidades_conhecimentos_a_1_denominador)
		else 0
	end as qualidade_dados,
	case
		when (qualidade_padronizacao_1_denominador + tecnologia_metodos_1_denominador + habilidades_conhecimentos_a_1_denominador) > 0 then
        (qualidade_padronizacao_1_denominador + tecnologia_metodos_1_denominador + habilidades_conhecimentos_a_1_denominador)
		else 0
	end as qualidade_dados_3_denominador,
	--calcula analise_dados_insight
case
		when (aplicativos_ferramentas_1_denominador + tecnicas_analises_1_denominador + habilidades_conhecimentos_b_1_denominador) > 0 then
        (aplicativos_ferramentas + tecnicas_analises + habilidades_conhecimentos_b) /
        (aplicativos_ferramentas_1_denominador + tecnicas_analises_1_denominador + habilidades_conhecimentos_b_1_denominador)
		else 0
	end as analise_dados_insight,
	case
		when (aplicativos_ferramentas_1_denominador + tecnicas_analises_1_denominador + habilidades_conhecimentos_b_1_denominador) > 0 then
        (aplicativos_ferramentas_1_denominador + tecnicas_analises_1_denominador + habilidades_conhecimentos_b_1_denominador)
		else 0
	end as analise_dados_insight_3_denominador,
	--calcula uso_informacoes
case
		when (requisitos_uso_1_denominador + gestao_conhecimento_1_denominador + governanca_informacao_1_denominador) > 0 then
        (requisitos_uso + gestao_conhecimento + governanca_informacao) /
        (requisitos_uso_1_denominador + gestao_conhecimento_1_denominador + governanca_informacao_1_denominador)
		else 0
	end as uso_informacoes,
	case
		when (requisitos_uso_1_denominador + gestao_conhecimento_1_denominador + governanca_informacao_1_denominador) > 0 then
        (requisitos_uso_1_denominador + gestao_conhecimento_1_denominador + governanca_informacao_1_denominador)
		else 0
	end as uso_informacoes_3_denominador,
	--calcula tomada_decisao
case
		when (metas_resultados_1_denominador + processo_tomada_decisao_1_denominador + lideranca_empoderamento_1_denominador) > 0 then
        (metas_resultados + processo_tomada_decisao + lideranca_empoderamento) /
        (metas_resultados_1_denominador + processo_tomada_decisao_1_denominador + lideranca_empoderamento_1_denominador)
		else 0
	end as tomada_decisao,
	case
		when (metas_resultados_1_denominador + processo_tomada_decisao_1_denominador + lideranca_empoderamento_1_denominador) > 0 then
        (metas_resultados_1_denominador + processo_tomada_decisao_1_denominador + lideranca_empoderamento_1_denominador)
		else 0
	end as tomada_decisao_3_denominador
	from denominadores_atributos)
	--------------------------------------
	-- PARTE 8 ---------
	-- CHAMAR calculo_dimensoes
	--------------------------------------
, denominadores_dimensoes as (
select
	V0101,
	disponibilidade_dados,
	case
		when disponibilidade_dados_3_denominador > 0 then 1
		else 0
	end as disponibilidade_dados_1_denominador,
	qualidade_dados,
	case
		when qualidade_dados_3_denominador > 0 then 1
		else 0
	end as qualidade_dados_1_denominador,
	analise_dados_insight,
	case
		when analise_dados_insight_3_denominador > 0 then 1
		else 0
	end as analise_dados_insight_1_denominador,
	uso_informacoes,
	case
		when uso_informacoes_3_denominador > 0 then 1
		else 0
	end as uso_informacoes_1_denominador,
	tomada_decisao,
	case
		when tomada_decisao_3_denominador > 0 then 1
		else 0
	end as tomada_decisao_1_denominador
	from calculo_dimensoes)	
	--------------------------------------
	-- PARTE 9 ---------
	-- CHAMAR denominadores_dimensoes
	--------------------------------------
, calculo_indice as (
select
	V0101,
	case
		when (disponibilidade_dados_1_denominador + qualidade_dados_1_denominador + analise_dados_insight_1_denominador + uso_informacoes_1_denominador + tomada_decisao_1_denominador) > 0 then
        (disponibilidade_dados + qualidade_dados + analise_dados_insight + uso_informacoes + tomada_decisao) /
        (disponibilidade_dados_1_denominador + qualidade_dados_1_denominador + analise_dados_insight_1_denominador + uso_informacoes_1_denominador + tomada_decisao_1_denominador)
		else 0
	end as indice,
	case
		when (disponibilidade_dados_1_denominador + qualidade_dados_1_denominador + analise_dados_insight_1_denominador + uso_informacoes_1_denominador + tomada_decisao_1_denominador) > 0 then
        (disponibilidade_dados_1_denominador + qualidade_dados_1_denominador + analise_dados_insight_1_denominador + uso_informacoes_1_denominador + tomada_decisao_1_denominador)
		else 0
	end as indice_5_denominador
	from denominadores_dimensoes)
	--------------------------------------
	-- PARTE 10 ---------
	-- CHAMAR calculo_indice
	--------------------------------------
, juncao_questoes_finais as (
select
	'Final' as temporalidade,
	-- QUESTOES AVALIATIVAS
a.V0101, QA101, QA102, QA103, QA13, QA8, QA104, QA105, QA106, QA16, QA17, QA107,QA118, QA18, QA20, QA21, QA23, QA19, QA108, QA22, QA24, QA109, QA34, QA25, QA32, QA29, QA110, QA31, QA33, QA35, QA36, QA37, QA38, QA39, QA111, QA112, QA40, QA42, QA43, QA44, QA50, QA113, QA114, QA51, QA52, QA53, QA115, QA66, QA58, QA59, QA63, QA116, QA117, QA65,
-- ATRIBUTOS
infraestrutura, governanca, propriedades, qualidade_padronizacao, tecnologia_metodos, habilidades_conhecimentos_a, aplicativos_ferramentas, tecnicas_analises, habilidades_conhecimentos_b, requisitos_uso, gestao_conhecimento, governanca_informacao, metas_resultados, processo_tomada_decisao, lideranca_empoderamento,
-- DIMENSOES
disponibilidade_dados, qualidade_dados, analise_dados_insight, uso_informacoes, tomada_decisao,
-- INDICADOR FINAL
indice
from
calculo_qa a
inner join calculo_atributos  b
on
a.V0101 = b.V0101
inner join calculo_dimensoes c
on
a.V0101 = c.V0101
inner join calculo_indice d
on
a.V0101 = d.V0101)
	--------------------------------------
	-- PARTE 11 ---------
	-- CHAMAR juncao_indice
	--------------------------------------
--, juncao_inicial_final as (
--select
--	*
--from
--	“Endereço do arquivo Junção de QAs,
--	Atributos,
--	Dimensões e Índice por rede Retrospectivo”
--union 
--select
--	*
--from
--	“Endereço do arquivo Junção de QAs,
--	Atributos,
--	Dimensões e Índice por rede Final”)
	--------------------------------------
	-- PARTE 12 ---------
	-- CHAMAR juncao_inicial_final
	--------------------------------------
--, empilhamento_final as (
--SELECT * FROM (SELECT V0101, temporalidade, infraestrutura, governanca, propriedades, qualidade_padronizacao, tecnologia_metodos, habilidades_conhecimentos_a, aplicativos_ferramentas, tecnicas_analises, habilidades_conhecimentos_b, requisitos_uso, gestao_conhecimento, governanca_informacao, metas_resultados, processo_tomada_decisao, lideranca_empoderamento FROM “Endereço do arquivo de Junção dos arquivos Retrospectivo e Final")
--UNPIVOT (
--    nota FOR atributo IN (infraestrutura, governanca, propriedades, qualidade_padronizacao, tecnologia_metodos, habilidades_conhecimentos_a, aplicativos_ferramentas, tecnicas_analises, habilidades_conhecimentos_b, requisitos_uso, gestao_conhecimento, governanca_informacao, metas_resultados, processo_tomada_decisao, lideranca_empoderamento)
--)
select * from juncao_questoes_finais  
--------------------------------------
-- parte 3 media_por_rede
-- parte 4 denominadores
-- parte 5 calculo_qa
-- parte 6 denominadores_qa
-- parte 7 calculo_atributos
-- parte 8 denominadores_atributos 
-- parte 9 calculo_dimensoes
-- parte 10 denominadores_dimensoes 
-- parte 11 juncao_indice